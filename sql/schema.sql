CREATE TABLE IF NOT EXISTS orders (
  id          UUID PRIMARY KEY,
  customer_id TEXT NOT NULL,
  amount      NUMERIC(12,2) NOT NULL,
  status      TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox_events (
  id              UUID PRIMARY KEY,
  aggregate_type  TEXT NOT NULL,
  aggregate_id    TEXT NOT NULL,
  event_type      TEXT NOT NULL,
  payload         JSONB NOT NULL,

  status          TEXT NOT NULL DEFAULT 'NEW', -- NEW | SENT | DEAD
  attempts        INT  NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  locked_at       TIMESTAMPTZ,
  locked_by       TEXT,

  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at         TIMESTAMPTZ,
  last_error      TEXT
);

CREATE INDEX IF NOT EXISTS idx_outbox_pick
  ON outbox_events (status, next_attempt_at, created_at);

CREATE INDEX IF NOT EXISTS idx_outbox_agg
  ON outbox_events (aggregate_type, aggregate_id);
