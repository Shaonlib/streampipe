package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/Shaonlib/streampipe/internal/decoder"
)

// StdoutSink writes JSON-encoded change events to stdout.
// Pipe to `jq` for pretty-printing during development.
type StdoutSink struct{}

func NewStdout() *StdoutSink { return &StdoutSink{} }

type envelope struct {
	LSN    string         `json:"lsn"`
	Time   time.Time      `json:"time"`
	Schema string         `json:"schema"`
	Table  string         `json:"table"`
	Op     decoder.Operation `json:"op"`
	Before map[string]any `json:"before,omitempty"`
	After  map[string]any `json:"after,omitempty"`
}

func (s *StdoutSink) Write(_ context.Context, event *decoder.ChangeEvent) error {
	e := envelope{
		LSN:    event.LSN.String(),
		Time:   event.Timestamp,
		Schema: event.Schema,
		Table:  event.Table,
		Op:     event.Op,
		Before: event.Before,
		After:  event.After,
	}
	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("stdout sink marshal: %w", err)
	}
	_, err = fmt.Fprintln(os.Stdout, string(b))
	return err
}

func (s *StdoutSink) Close() error { return nil }
