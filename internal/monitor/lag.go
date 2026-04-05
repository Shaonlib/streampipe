package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/Shaonlib/streampipe/internal/config"
)

var (
	lagBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "streampipe_replication_lag_bytes",
		Help: "Replication lag in bytes between source WAL and confirmed flush LSN.",
	})
	confirmedLSN = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "streampipe_confirmed_lsn",
		Help: "Last confirmed flush LSN as a numeric XLogRecPtr.",
	})
	eventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "streampipe_events_total",
		Help: "Total change events processed, labelled by table and operation.",
	}, []string{"table", "op"})
	errorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "streampipe_errors_total",
		Help: "Total sink write errors.",
	})
)

// ReplicationStat is a snapshot of the replication slot state.
type ReplicationStat struct {
	SlotName         string
	Plugin           string
	Active           bool
	ConfirmedFlushLSN pglogrepl.LSN
	CurrentLSN       pglogrepl.LSN
	LagBytes         int64
}

// Monitor polls pg_stat_replication and pg_replication_slots and exposes
// metrics via Prometheus.
type Monitor struct {
	cfg  *config.Config
	pool *pgxpool.Pool
}

func New(cfg *config.Config) (*Monitor, error) {
	pool, err := pgxpool.New(context.Background(), cfg.Source.RegularDSN())
	if err != nil {
		return nil, fmt.Errorf("monitor connect: %w", err)
	}
	return &Monitor{cfg: cfg, pool: pool}, nil
}

func (m *Monitor) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(m.cfg.Monitor.PollIntervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stat, err := m.poll(ctx)
			if err != nil {
				slog.Warn("monitor poll error", "err", err)
				continue
			}
			lagBytes.Set(float64(stat.LagBytes))
			confirmedLSN.Set(float64(stat.ConfirmedFlushLSN))

			if stat.LagBytes > m.cfg.Monitor.LagWarnBytes {
				slog.Warn("replication lag exceeds threshold",
					"lag_bytes", stat.LagBytes,
					"threshold_bytes", m.cfg.Monitor.LagWarnBytes,
					"slot", stat.SlotName,
				)
			}
		}
	}
}

func (m *Monitor) poll(ctx context.Context) (*ReplicationStat, error) {
	row := m.pool.QueryRow(ctx, `
		SELECT
			s.slot_name,
			s.plugin,
			s.active,
			s.confirmed_flush_lsn,
			pg_current_wal_lsn(),
			pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn) AS lag_bytes
		FROM pg_replication_slots s
		WHERE s.slot_name = $1
	`, m.cfg.Source.SlotName)

	var stat ReplicationStat
	var flushStr, currentStr string
	err := row.Scan(
		&stat.SlotName,
		&stat.Plugin,
		&stat.Active,
		&flushStr,
		&currentStr,
		&stat.LagBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("scan replication stat: %w", err)
	}

	stat.ConfirmedFlushLSN, _ = pglogrepl.ParseLSN(flushStr)
	stat.CurrentLSN, _ = pglogrepl.ParseLSN(currentStr)
	return &stat, nil
}

// RecordEvent increments the per-table/op Prometheus counter.
func RecordEvent(table, op string) {
	eventsTotal.WithLabelValues(table, op).Inc()
}

// RecordError increments the error counter.
func RecordError() {
	errorsTotal.Inc()
}

func (m *Monitor) Close() {
	m.pool.Close()
}
