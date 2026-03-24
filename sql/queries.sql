-- name: CreateOrder :exec
INSERT INTO orders (id, customer_id, amount, status)
VALUES ($1, $2, $3, $4);

-- name: InsertOutboxEvent :exec
INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload)
VALUES ($1, $2, $3, $4, $5);

-- name: ClaimOutboxBatch :many
SELECT id, aggregate_type, aggregate_id, event_type, payload, attempts
FROM outbox_events
WHERE status = 'NEW'
  AND next_attempt_at <= now()
ORDER BY created_at
LIMIT $1
FOR UPDATE SKIP LOCKED;

-- name: LockOutboxEvent :exec
UPDATE outbox_events
SET locked_at = now(),
    locked_by = $2
WHERE id = $1;

-- name: MarkOutboxSent :exec
UPDATE outbox_events
SET status='SENT',
    sent_at=now(),
    locked_at=NULL,
    locked_by=NULL,
    last_error=NULL
WHERE id=$1;

-- name: MarkOutboxFailed :exec
UPDATE outbox_events
SET status=$2,
    attempts=attempts+1,
    next_attempt_at=$3,
    locked_at=NULL,
    locked_by=NULL,
    last_error=$4
WHERE id=$1;
