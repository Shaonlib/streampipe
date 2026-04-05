package sink

import (
	"context"

	"github.com/Shaonlib/streampipe/internal/config"
	"github.com/Shaonlib/streampipe/internal/decoder"
)

// Sink receives decoded change events and writes them to a destination.
type Sink interface {
	// Write persists a single change event. Implementations must be safe to
	// call from a single goroutine (the pipeline fan-out loop).
	Write(ctx context.Context, event *decoder.ChangeEvent) error
	// Close flushes buffers and releases resources.
	Close() error
}

// ApplyTransforms mutates the event's After map in-place by redacting
// configured columns (replacing their values with "***REDACTED***").
func ApplyTransforms(event *decoder.ChangeEvent, rules []config.TransformRule) {
	for _, rule := range rules {
		if rule.Table != event.TableName() {
			continue
		}
		for _, col := range rule.Redact {
			if event.After != nil {
				if _, ok := event.After[col]; ok {
					event.After[col] = "***REDACTED***"
				}
			}
			if event.Before != nil {
				if _, ok := event.Before[col]; ok {
					event.Before[col] = "***REDACTED***"
				}
			}
		}
	}
}
