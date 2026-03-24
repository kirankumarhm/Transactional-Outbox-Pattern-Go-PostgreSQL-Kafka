package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"transactional-outbox/internal/config"
)

func NewPool(ctx context.Context, dbCfg config.DBConfig) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dbCfg.URL)
	if err != nil {
		return nil, err
	}

	cfg.MaxConns = dbCfg.MaxConns
	cfg.MinConns = dbCfg.MinConns
	cfg.MaxConnLifetime = dbCfg.MaxConnLifetime
	cfg.MaxConnIdleTime = dbCfg.MaxConnIdleTime

	return pgxpool.NewWithConfig(ctx, cfg)
}
