package sink

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/Shaonlib/streampipe/internal/config"
	"github.com/Shaonlib/streampipe/internal/decoder"
)

type PostgresSink struct {
	pool *pgxpool.Pool
}

func NewPostgres(ctx context.Context, cfg config.PostgresSink) (*PostgresSink, error) {
	pool, err := pgxpool.New(ctx, cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("postgres sink connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres sink ping: %w", err)
	}
	slog.Info("postgres sink connected", "dsn", cfg.DSN())
	return &PostgresSink{pool: pool}, nil
}

func (s *PostgresSink) Write(ctx context.Context, event *decoder.ChangeEvent) error {
	switch event.Op {
	case decoder.OpInsert, decoder.OpUpdate:
		return s.upsert(ctx, event)
	case decoder.OpDelete:
		return s.delete(ctx, event)
	}
	return nil
}

func (s *PostgresSink) upsert(ctx context.Context, event *decoder.ChangeEvent) error {
	row := event.After
	if len(row) == 0 {
		return nil
	}

	cols := make([]string, 0, len(row))
	vals := make([]any, 0, len(row))
	placeholders := make([]string, 0, len(row))
	updates := make([]string, 0, len(row))

	i := 1
	for col, val := range row {
		cols = append(cols, pgx.Identifier{col}.Sanitize())
		vals = append(vals, val)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s",
			pgx.Identifier{col}.Sanitize(),
			pgx.Identifier{col}.Sanitize()))
		i++
	}

	table := pgx.Identifier{event.Schema, event.Table}.Sanitize()
	sql := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (id) DO UPDATE SET %s`,
		table,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updates, ", "),
	)

	_, err := s.pool.Exec(ctx, sql, vals...)
	if err != nil {
		return fmt.Errorf("upsert %s: %w", event.TableName(), err)
	}
	return nil
}

func (s *PostgresSink) delete(ctx context.Context, event *decoder.ChangeEvent) error {
	row := event.Before
	if len(row) == 0 {
		slog.Warn("DELETE event has no before image", "table", event.TableName())
		return nil
	}

	wheres := make([]string, 0)
	vals := make([]any, 0)
	i := 1
	for col, val := range row {
		wheres = append(wheres, fmt.Sprintf("%s = $%d", pgx.Identifier{col}.Sanitize(), i))
		vals = append(vals, val)
		i++
	}

	table := pgx.Identifier{event.Schema, event.Table}.Sanitize()
	sql := fmt.Sprintf(`DELETE FROM %s WHERE %s`, table, strings.Join(wheres, " AND "))
	_, err := s.pool.Exec(ctx, sql, vals...)
	if err != nil {
		return fmt.Errorf("delete %s: %w", event.TableName(), err)
	}
	return nil
}

func (s *PostgresSink) Close() error {
	s.pool.Close()
	return nil
}
