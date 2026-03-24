package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server ServerConfig `yaml:"server"`
	DB     DBConfig     `yaml:"db"`
	Kafka  KafkaConfig  `yaml:"kafka"`
	Outbox OutboxConfig `yaml:"outbox"`
}

type ServerConfig struct {
	Port              string        `yaml:"port"`
	ReadHeaderTimeout time.Duration `yaml:"-"`
	RawReadHeader     string        `yaml:"readHeaderTimeout"`
	ShutdownTimeout   time.Duration `yaml:"-"`
	RawShutdown       string        `yaml:"shutdownTimeout"`
}

type DBConfig struct {
	URL             string        `yaml:"url"`
	MaxConns        int32         `yaml:"maxConns"`
	MinConns        int32         `yaml:"minConns"`
	MaxConnLifetime time.Duration `yaml:"-"`
	RawLifetime     string        `yaml:"maxConnLifetime"`
	MaxConnIdleTime time.Duration `yaml:"-"`
	RawIdleTime     string        `yaml:"maxConnIdleTime"`
}

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

type OutboxConfig struct {
	BatchSize   int           `yaml:"batchSize"`
	Interval    time.Duration `yaml:"-"`
	RawInterval string        `yaml:"interval"`
	MaxAttempts int           `yaml:"maxAttempts"`
}

func Load() (Config, error) {
	path := envStr("CONFIG_PATH", "config.yaml")

	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("reading config file %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parsing config file: %w", err)
	}

	applyEnvOverrides(&cfg)

	if err := parseDurations(&cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func applyEnvOverrides(cfg *Config) {
	overrideStr("SERVER_PORT", &cfg.Server.Port)
	overrideStr("SERVER_READ_HEADER_TIMEOUT", &cfg.Server.RawReadHeader)
	overrideStr("SERVER_SHUTDOWN_TIMEOUT", &cfg.Server.RawShutdown)

	overrideStr("DATABASE_URL", &cfg.DB.URL)
	overrideInt32("DB_MAX_CONNS", &cfg.DB.MaxConns)
	overrideInt32("DB_MIN_CONNS", &cfg.DB.MinConns)
	overrideStr("DB_MAX_CONN_LIFETIME", &cfg.DB.RawLifetime)
	overrideStr("DB_MAX_CONN_IDLE_TIME", &cfg.DB.RawIdleTime)

	overrideStr("KAFKA_BROKERS", &cfg.Kafka.Brokers)
	overrideStr("KAFKA_TOPIC", &cfg.Kafka.Topic)
	overrideInt("KAFKA_RETRIES", &cfg.Kafka.Retries)
	overrideInt("KAFKA_LINGER_MS", &cfg.Kafka.LingerMs)
	overrideInt("KAFKA_BATCH_NUM_MESSAGES", &cfg.Kafka.BatchNumMessages)
	overrideStr("KAFKA_COMPRESSION_TYPE", &cfg.Kafka.CompressionType)
	overrideInt("KAFKA_MAX_SEND_RETRIES", &cfg.Kafka.MaxSendRetries)
	overrideInt("KAFKA_FLUSH_TIMEOUT_MS", &cfg.Kafka.FlushTimeoutMs)

	overrideInt("OUTBOX_BATCH_SIZE", &cfg.Outbox.BatchSize)
	overrideStr("OUTBOX_INTERVAL", &cfg.Outbox.RawInterval)
	overrideInt("OUTBOX_MAX_ATTEMPTS", &cfg.Outbox.MaxAttempts)
}

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

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func overrideStr(key string, target *string) {
	if v := os.Getenv(key); v != "" {
		*target = v
	}
}

func overrideInt(key string, target *int) {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			*target = i
		}
	}
}

func overrideInt32(key string, target *int32) {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			*target = int32(i)
		}
	}
}
