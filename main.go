package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shaonlib/streampipe/internal/api"
	"github.com/Shaonlib/streampipe/internal/config"
	"github.com/Shaonlib/streampipe/internal/monitor"
	"github.com/Shaonlib/streampipe/internal/replication"
	"github.com/Shaonlib/streampipe/internal/sink"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if err := run(*configPath); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Build the sink.
	s, err := buildSink(ctx, cfg)
	if err != nil {
		return err
	}
	defer s.Close()

	// Start the replication consumer.
	consumer := replication.New(cfg)

	// Start the lag monitor.
	mon, err := monitor.New(cfg)
	if err != nil {
		slog.Warn("monitor init failed — metrics will be unavailable", "err", err)
	} else {
		defer mon.Close()
		go mon.Run(ctx)
	}

	// Start the HTTP API.
	apiServer := api.New(cfg, consumer)
	go func() {
		if err := apiServer.Run(ctx); err != nil {
			slog.Error("api server error", "err", err)
		}
	}()

	// Fan-out: consume events and write to sink.
	go func() {
		for event := range consumer.Events() {
			sink.ApplyTransforms(event, cfg.Transforms)
			if err := s.Write(ctx, event); err != nil {
				slog.Error("sink write error",
					"table", event.TableName(),
					"op", event.Op,
					"err", err,
				)
				monitor.RecordError()
				continue
			}
			monitor.RecordEvent(event.TableName(), string(event.Op))
			slog.Debug("event written",
				"table", event.TableName(),
				"op", event.Op,
				"lsn", event.LSN,
			)
		}
	}()

	// Run the replication consumer (blocks until ctx is cancelled).
	slog.Info("StreamPipe started", "config", configPath, "sink", cfg.Sink.Type)
	return consumer.Run(ctx)
}

func buildSink(ctx context.Context, cfg *config.Config) (sink.Sink, error) {
	switch cfg.Sink.Type {
	case "postgres":
		return sink.NewPostgres(ctx, cfg.Sink.Postgres)
	case "file":
		if cfg.Sink.File.Path == "" {
			return nil, fmt.Errorf("sink.file.path is required for file sink")
		}
		format := cfg.Sink.File.Format
		if format == "" {
			format = "jsonl"
		}
		return sink.NewFile(cfg.Sink.File.Path, format)
	case "stdout":
		return sink.NewStdout(), nil
	default:
		return nil, fmt.Errorf("unknown sink type: %s", cfg.Sink.Type)
	}
}
