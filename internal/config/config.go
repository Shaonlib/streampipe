package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Source     SourceConfig    `yaml:"source"`
	Sink       SinkConfig      `yaml:"sink"`
	Tables     []string        `yaml:"tables"`
	Transforms []TransformRule `yaml:"transforms"`
	API        APIConfig       `yaml:"api"`
	Monitor    MonitorConfig   `yaml:"monitor"`
}

type SourceConfig struct {
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	User            string `yaml:"user"`
	Password        string `yaml:"password"`
	Database        string `yaml:"database"`
	SlotName        string `yaml:"slot_name"`
	PublicationName string `yaml:"publication_name"`
}

func (s SourceConfig) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database",
		s.User, s.Password, s.Host, s.Port, s.Database)
}

func (s SourceConfig) RegularDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		s.User, s.Password, s.Host, s.Port, s.Database)
}

type SinkConfig struct {
	Type     string         `yaml:"type"`
	Postgres PostgresSink   `yaml:"postgres"`
	File     FileSink       `yaml:"file"`
}

type PostgresSink struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

func (p PostgresSink) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		p.User, p.Password, p.Host, p.Port, p.Database)
}

type FileSink struct {
	Path   string `yaml:"path"`
	Format string `yaml:"format"`
}

type TransformRule struct {
	Table  string   `yaml:"table"`
	Redact []string `yaml:"redact"`
}

type APIConfig struct {
	Port int `yaml:"port"`
}

type MonitorConfig struct {
	LagWarnBytes        int64 `yaml:"lag_warn_bytes"`
	PollIntervalSeconds int   `yaml:"poll_interval_seconds"`
}

func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening config: %w", err)
	}
	defer f.Close()

	var cfg Config
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	if err := validate(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func validate(cfg *Config) error {
	if cfg.Source.SlotName == "" {
		return fmt.Errorf("source.slot_name is required")
	}
	if cfg.Source.PublicationName == "" {
		return fmt.Errorf("source.publication_name is required")
	}
	if len(cfg.Tables) == 0 {
		return fmt.Errorf("at least one table must be specified")
	}
	validSinks := map[string]bool{"postgres": true, "file": true, "stdout": true}
	if !validSinks[cfg.Sink.Type] {
		return fmt.Errorf("sink.type must be one of: postgres, file, stdout")
	}
	if cfg.API.Port == 0 {
		cfg.API.Port = 8080
	}
	if cfg.Monitor.PollIntervalSeconds == 0 {
		cfg.Monitor.PollIntervalSeconds = 5
	}
	return nil
}
