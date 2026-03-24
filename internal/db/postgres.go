// Package db provides PostgreSQL connection pool initialization.
package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"transactional-outbox/internal/config"
)

// NewPool creates a pgx connection pool configured from the provided DBConfig.
// It sets pool size limits and connection lifetime/idle timeouts to control
// resource usage against PostgreSQL.
func NewPool(ctx context.Context, dbCfg config.DBConfig) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dbCfg.URL)
	if err != nil {
		return nil, err
	}

	cfg.MaxConns = dbCfg.MaxConns         // upper bound on open connections
	cfg.MinConns = dbCfg.MinConns         // keep at least this many idle connections warm
	cfg.MaxConnLifetime = dbCfg.MaxConnLifetime // recycle connections after this duration
	cfg.MaxConnIdleTime = dbCfg.MaxConnIdleTime // close idle connections after this duration

	return pgxpool.NewWithConfig(ctx, cfg)
}
