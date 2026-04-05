package replication

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/Shaonlib/streampipe/internal/config"
	"github.com/Shaonlib/streampipe/internal/decoder"
)

const standbyStatusInterval = 10 * time.Second

// Consumer opens a PostgreSQL logical replication connection, creates (or
// reuses) the replication slot and publication, and streams WAL changes.
type Consumer struct {
	cfg     *config.Config
	dec     *decoder.Decoder
	conn    *pgconn.PgConn
	lsn     atomic.Uint64 // last confirmed LSN
	paused  atomic.Bool
	eventCh chan *decoder.ChangeEvent
}

func New(cfg *config.Config) *Consumer {
	return &Consumer{
		cfg:     cfg,
		dec:     decoder.New(),
		eventCh: make(chan *decoder.ChangeEvent, 512),
	}
}

// Events returns the read-only channel of decoded change events.
func (c *Consumer) Events() <-chan *decoder.ChangeEvent {
	return c.eventCh
}

// LSN returns the last confirmed LSN as a uint64 (XLogRecPtr).
func (c *Consumer) LSN() uint64 {
	return c.lsn.Load()
}

// Pause suspends forwarding events to the channel (WAL is still consumed).
func (c *Consumer) Pause() { c.paused.Store(true) }

// Resume re-enables forwarding.
func (c *Consumer) Resume() { c.paused.Store(false) }

// Run connects, ensures the slot + publication exist, then streams indefinitely.
// It returns only when ctx is cancelled or a non-recoverable error occurs.
func (c *Consumer) Run(ctx context.Context) error {
	conn, err := pgconn.Connect(ctx, c.cfg.Source.DSN())
	if err != nil {
		return fmt.Errorf("replication connect: %w", err)
	}
	c.conn = conn
	defer conn.Close(ctx)

	if err := c.ensurePublication(ctx); err != nil {
		return err
	}

	startLSN, err := c.ensureSlot(ctx)
	if err != nil {
		return err
	}

	slog.Info("starting logical replication",
		"slot", c.cfg.Source.SlotName,
		"publication", c.cfg.Source.PublicationName,
		"start_lsn", startLSN,
	)

	pluginArgs := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", c.cfg.Source.PublicationName),
		"messages 'true'",
		"streaming 'false'",
	}

	if err := pglogrepl.StartReplication(ctx, conn, c.cfg.Source.SlotName, startLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	return c.streamLoop(ctx)
}

// streamLoop reads WAL messages and dispatches decoded events.
func (c *Consumer) streamLoop(ctx context.Context) error {
	nextStandby := time.Now().Add(standbyStatusInterval)

	for {
		if time.Now().After(nextStandby) {
			lsn := pglogrepl.LSN(c.lsn.Load())
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, c.conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn}); err != nil {
				return fmt.Errorf("standby status update: %w", err)
			}
			nextStandby = time.Now().Add(standbyStatusInterval)
		}

		recvCtx, cancel := context.WithDeadline(ctx, nextStandby)
		rawMsg, err := c.conn.ReceiveMessage(recvCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if ctx.Err() != nil {
				return nil // clean shutdown
			}
			return fmt.Errorf("receive message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("postgres error: %s (code=%s)", errMsg.Message, errMsg.Code)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				slog.Warn("parse keepalive", "err", err)
				continue
			}
			if pkm.ReplyRequested {
				nextStandby = time.Time{} // force immediate standby reply
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				slog.Warn("parse xlogdata", "err", err)
				continue
			}

			walMsg, err := pglogrepl.ParseV2(xld.WALData, false)
			if err != nil {
				slog.Warn("parse WAL message", "err", err)
				continue
			}

			event, err := c.dec.Decode(xld.WALStart, walMsg)
			if err != nil {
				slog.Warn("decode WAL message", "err", err)
				continue
			}

			// Advance confirmed LSN regardless of pause state.
			if uint64(xld.WALStart) > c.lsn.Load() {
				c.lsn.Store(uint64(xld.WALStart))
			}

			if event != nil && !c.paused.Load() {
				select {
				case c.eventCh <- event:
				case <-ctx.Done():
					return nil
				}
			}
		}
	}
}

// ensurePublication creates the publication if it doesn't exist.
func (c *Consumer) ensurePublication(ctx context.Context) error {
	// We need a regular (non-replication) connection for DDL.
	ddlConn, err := pgconn.Connect(ctx, c.cfg.Source.RegularDSN())
	if err != nil {
		return fmt.Errorf("ddl connect: %w", err)
	}
	defer ddlConn.Close(ctx)

	// Build table list for FOR TABLE clause.
	tables := ""
	for i, t := range c.cfg.Tables {
		if i > 0 {
			tables += ", "
		}
		tables += t
	}

	sql := fmt.Sprintf(
		`DO $$ BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '%s') THEN
				CREATE PUBLICATION %s FOR TABLE %s;
			END IF;
		END $$;`,
		c.cfg.Source.PublicationName,
		c.cfg.Source.PublicationName,
		tables,
	)

	result := ddlConn.Exec(ctx, sql)
	_, err = result.ReadAll()
	if err != nil {
		return fmt.Errorf("ensure publication: %w", err)
	}
	slog.Info("publication ready", "name", c.cfg.Source.PublicationName)
	return nil
}

// ensureSlot creates the replication slot if it doesn't exist, and returns
// the LSN to start streaming from.
func (c *Consumer) ensureSlot(ctx context.Context) (pglogrepl.LSN, error) {
	// Check if slot already exists.
	result, err := pglogrepl.IdentifySystem(ctx, c.conn)
	if err != nil {
		return 0, fmt.Errorf("identify system: %w", err)
	}

	// Query existing slots via a regular connection.
	ddlConn, err := pgconn.Connect(ctx, c.cfg.Source.RegularDSN())
	if err != nil {
		return 0, fmt.Errorf("ddl connect for slot check: %w", err)
	}
	defer ddlConn.Close(ctx)

	rows, err := ddlConn.Exec(ctx,
		fmt.Sprintf(`SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s'`,
			c.cfg.Source.SlotName)).ReadAll()
	if err != nil {
		return 0, fmt.Errorf("query replication slot: %w", err)
	}

	if len(rows) > 0 && len(rows[0].Rows) > 0 {
		// Slot exists — resume from confirmed_flush_lsn.
		lsnStr := string(rows[0].Rows[0][0])
		lsn, err := pglogrepl.ParseLSN(lsnStr)
		if err != nil {
			return 0, fmt.Errorf("parse confirmed_flush_lsn: %w", err)
		}
		slog.Info("resuming from existing slot", "slot", c.cfg.Source.SlotName, "lsn", lsn)
		c.lsn.Store(uint64(lsn))
		return lsn, nil
	}

	// Create new slot.
	slotResult, err := pglogrepl.CreateReplicationSlot(ctx, c.conn,
		c.cfg.Source.SlotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil {
		return 0, fmt.Errorf("create replication slot: %w", err)
	}

	lsn, err := pglogrepl.ParseLSN(slotResult.ConsistentPoint)
	if err != nil {
		return 0, fmt.Errorf("parse slot LSN: %w", err)
	}

	slog.Info("created replication slot",
		"slot", c.cfg.Source.SlotName,
		"consistent_point", lsn,
		"system_lsn", result.XLogPos,
	)
	c.lsn.Store(uint64(lsn))
	return lsn, nil
}
