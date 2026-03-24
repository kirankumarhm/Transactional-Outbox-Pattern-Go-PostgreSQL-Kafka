// Package config handles externalized configuration loading.
// It reads from a YAML file (default: config.yaml) and allows every value
// to be overridden by environment variables at runtime — no rebuild required.
//
// Loading priority (highest wins):
//  1. Environment variable (e.g. KAFKA_BROKERS=broker:9092)
//  2. config.yaml value
//
// The YAML file path itself is configurable via the CONFIG_PATH env var.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level application configuration, composed of
// server, database, Kafka, and outbox publisher settings.
type Config struct {
	Server ServerConfig `yaml:"server"`
	DB     DBConfig     `yaml:"db"`
	Kafka  KafkaConfig  `yaml:"kafka"`
	Outbox OutboxConfig `yaml:"outbox"`
}

// ServerConfig holds HTTP server settings.
// Duration fields are parsed from their Raw* string counterparts after loading.
type ServerConfig struct {
	Port              string        `yaml:"port"`
	ReadHeaderTimeout time.Duration `yaml:"-"`              // parsed from RawReadHeader
	RawReadHeader     string        `yaml:"readHeaderTimeout"`
	ShutdownTimeout   time.Duration `yaml:"-"`              // parsed from RawShutdown
	RawShutdown       string        `yaml:"shutdownTimeout"`
}

// DBConfig holds PostgreSQL connection pool settings.
type DBConfig struct {
	URL             string        `yaml:"url"`
	MaxConns        int32         `yaml:"maxConns"`
	MinConns        int32         `yaml:"minConns"`
	MaxConnLifetime time.Duration `yaml:"-"`             // parsed from RawLifetime
	RawLifetime     string        `yaml:"maxConnLifetime"`
	MaxConnIdleTime time.Duration `yaml:"-"`             // parsed from RawIdleTime
	RawIdleTime     string        `yaml:"maxConnIdleTime"`
}

// KafkaConfig holds Kafka/Redpanda producer settings.
type KafkaConfig struct {
	Brokers          string `yaml:"brokers"`
	Topic            string `yaml:"topic"`
	Retries          int    `yaml:"retries"`
	LingerMs         int    `yaml:"lingerMs"`
	BatchNumMessages int    `yaml:"batchNumMessages"`
	CompressionType  string `yaml:"compressionType"`
	MaxSendRetries   int    `yaml:"maxSendRetries"`
	FlushTimeoutMs   int    `yaml:"flushTimeoutMs"`
}

// OutboxConfig holds outbox publisher polling settings.
type OutboxConfig struct {
	BatchSize   int           `yaml:"batchSize"`
	Interval    time.Duration `yaml:"-"`          // parsed from RawInterval
	RawInterval string        `yaml:"interval"`
	MaxAttempts int           `yaml:"maxAttempts"`
}

// Load reads the YAML config file, applies environment variable overrides,
// and parses duration strings into time.Duration fields.
// Returns an error if the file cannot be read, parsed, or contains invalid durations.
func Load() (Config, error) {
	// CONFIG_PATH env var allows pointing to a different config file per environment.
	path := envStr("CONFIG_PATH", "config.yaml")

	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("reading config file %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parsing config file: %w", err)
	}

	// Environment variables override YAML values, enabling per-environment
	// configuration without modifying the file (e.g. in containers).
	applyEnvOverrides(&cfg)

	// Convert raw duration strings (e.g. "5s", "500ms") into time.Duration.
	if err := parseDurations(&cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// applyEnvOverrides checks each supported environment variable and, if set,
// overwrites the corresponding config field. This runs after YAML parsing
// so env vars always take precedence.
func applyEnvOverrides(cfg *Config) {
	// Server overrides
	overrideStr("SERVER_PORT", &cfg.Server.Port)
	overrideStr("SERVER_READ_HEADER_TIMEOUT", &cfg.Server.RawReadHeader)
	overrideStr("SERVER_SHUTDOWN_TIMEOUT", &cfg.Server.RawShutdown)

	// Database overrides
	overrideStr("DATABASE_URL", &cfg.DB.URL)
	overrideInt32("DB_MAX_CONNS", &cfg.DB.MaxConns)
	overrideInt32("DB_MIN_CONNS", &cfg.DB.MinConns)
	overrideStr("DB_MAX_CONN_LIFETIME", &cfg.DB.RawLifetime)
	overrideStr("DB_MAX_CONN_IDLE_TIME", &cfg.DB.RawIdleTime)

	// Kafka overrides
	overrideStr("KAFKA_BROKERS", &cfg.Kafka.Brokers)
	overrideStr("KAFKA_TOPIC", &cfg.Kafka.Topic)
	overrideInt("KAFKA_RETRIES", &cfg.Kafka.Retries)
	overrideInt("KAFKA_LINGER_MS", &cfg.Kafka.LingerMs)
	overrideInt("KAFKA_BATCH_NUM_MESSAGES", &cfg.Kafka.BatchNumMessages)
	overrideStr("KAFKA_COMPRESSION_TYPE", &cfg.Kafka.CompressionType)
	overrideInt("KAFKA_MAX_SEND_RETRIES", &cfg.Kafka.MaxSendRetries)
	overrideInt("KAFKA_FLUSH_TIMEOUT_MS", &cfg.Kafka.FlushTimeoutMs)

	// Outbox overrides
	overrideInt("OUTBOX_BATCH_SIZE", &cfg.Outbox.BatchSize)
	overrideStr("OUTBOX_INTERVAL", &cfg.Outbox.RawInterval)
	overrideInt("OUTBOX_MAX_ATTEMPTS", &cfg.Outbox.MaxAttempts)
}

// parseDurations converts all Raw* string fields (e.g. "5s", "30m") into
// their corresponding time.Duration fields. Returns a descriptive error
// if any duration string is invalid.
func parseDurations(cfg *Config) error {
	var err error
	if cfg.Server.ReadHeaderTimeout, err = time.ParseDuration(cfg.Server.RawReadHeader); err != nil {
		return fmt.Errorf("invalid SERVER_READ_HEADER_TIMEOUT: %w", err)
	}
	if cfg.Server.ShutdownTimeout, err = time.ParseDuration(cfg.Server.RawShutdown); err != nil {
		return fmt.Errorf("invalid SERVER_SHUTDOWN_TIMEOUT: %w", err)
	}
	if cfg.DB.MaxConnLifetime, err = time.ParseDuration(cfg.DB.RawLifetime); err != nil {
		return fmt.Errorf("invalid DB_MAX_CONN_LIFETIME: %w", err)
	}
	if cfg.DB.MaxConnIdleTime, err = time.ParseDuration(cfg.DB.RawIdleTime); err != nil {
		return fmt.Errorf("invalid DB_MAX_CONN_IDLE_TIME: %w", err)
	}
	if cfg.Outbox.Interval, err = time.ParseDuration(cfg.Outbox.RawInterval); err != nil {
		return fmt.Errorf("invalid OUTBOX_INTERVAL: %w", err)
	}
	return nil
}

// envStr returns the value of the environment variable named key,
// or fallback if the variable is not set or empty.
func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// overrideStr overwrites *target with the environment variable value
// if the variable is set and non-empty.
func overrideStr(key string, target *string) {
	if v := os.Getenv(key); v != "" {
		*target = v
	}
}

// overrideInt overwrites *target with the environment variable value
// parsed as an integer. Silently ignores non-numeric values.
func overrideInt(key string, target *int) {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			*target = i
		}
	}
}

// overrideInt32 overwrites *target with the environment variable value
// parsed as an int32. Silently ignores non-numeric values.
func overrideInt32(key string, target *int32) {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			*target = int32(i)
		}
	}
}
