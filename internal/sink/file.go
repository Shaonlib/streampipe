package sink

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/Shaonlib/streampipe/internal/decoder"
)

// FileSink appends change events to a file in JSONL or CSV format.
type FileSink struct {
	format string
	f      *os.File
	csv    *csv.Writer
	header bool // CSV header written?
}

func NewFile(path, format string) (*FileSink, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("file sink open %q: %w", path, err)
	}
	s := &FileSink{format: format, f: f}
	if format == "csv" {
		s.csv = csv.NewWriter(f)
	}
	return s, nil
}

func (s *FileSink) Write(_ context.Context, event *decoder.ChangeEvent) error {
	switch s.format {
	case "csv":
		return s.writeCSV(event)
	default:
		return s.writeJSONL(event)
	}
}

func (s *FileSink) writeJSONL(event *decoder.ChangeEvent) error {
	row := map[string]any{
		"lsn":    event.LSN.String(),
		"time":   event.Timestamp.Format(time.RFC3339Nano),
		"schema": event.Schema,
		"table":  event.Table,
		"op":     string(event.Op),
		"before": event.Before,
		"after":  event.After,
	}
	b, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("jsonl marshal: %w", err)
	}
	_, err = fmt.Fprintln(s.f, string(b))
	return err
}

func (s *FileSink) writeCSV(event *decoder.ChangeEvent) error {
	row := event.After
	if row == nil {
		row = event.Before
	}
	if row == nil {
		return nil
	}

	// Stable column order.
	cols := make([]string, 0, len(row))
	for k := range row {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	if !s.header {
		header := append([]string{"_lsn", "_time", "_schema", "_table", "_op"}, cols...)
		if err := s.csv.Write(header); err != nil {
			return err
		}
		s.header = true
	}

	record := []string{
		event.LSN.String(),
		event.Timestamp.Format(time.RFC3339),
		event.Schema,
		event.Table,
		string(event.Op),
	}
	for _, col := range cols {
		record = append(record, valueToString(row[col]))
	}
	if err := s.csv.Write(record); err != nil {
		return err
	}
	s.csv.Flush()
	return s.csv.Error()
}

func (s *FileSink) Close() error {
	if s.csv != nil {
		s.csv.Flush()
	}
	return s.f.Close()
}

func valueToString(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case bool:
		return strconv.FormatBool(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	default:
		b, _ := json.Marshal(val)
		return string(b)
	}
}
