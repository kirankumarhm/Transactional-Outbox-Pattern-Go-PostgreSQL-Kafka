package outbox

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"transactional-outbox/internal/db/sqlc"
)

// Publisher is the background outbox poller. It periodically queries the
// outbox_events table for unsent events, publishes them to Kafka, and
// marks them as SENT. Failed deliveries are retried with exponential backoff
// until MaxAttempts is reached, after which the event is marked DEAD.
type Publisher struct {
	Pool        *pgxpool.Pool  // database connection pool
	Q           *sqlc.Queries  // type-safe query executor
	Producer    *KafkaProducer // Kafka message producer
	WorkerID    string         // unique identifier for this publisher instance (used in locked_by)
	BatchSize   int            // max events to claim per polling cycle
	Interval    time.Duration  // how often to poll for new events
	MaxAttempts int            // max delivery attempts before marking DEAD
}

// Run starts the polling loop. It ticks every p.Interval and processes
// a batch of outbox events on each tick. It stops when ctx is cancelled
// (e.g. on application shutdown).
func (p *Publisher) Run(ctx context.Context) {
	t := time.NewTicker(p.Interval)
	defer t.Stop()

	log.Printf("[outbox] started worker=%s", p.WorkerID)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[outbox] stopping worker=%s", p.WorkerID)
			return
		case <-t.C:
			p.tick(ctx)
		}
	}
}

// tick runs one polling cycle: claim a batch of events, then attempt to
// publish each one to Kafka. Successful events are marked SENT; failed
// events are retried with backoff or marked DEAD.
func (p *Publisher) tick(ctx context.Context) {
	rows, err := p.claim(ctx)
	if err != nil {
		log.Printf("[outbox] claim error: %v", err)
		return
	}
	for _, r := range rows {
		id := r.ID.Bytes

		// Attach metadata as Kafka headers so consumers can route/filter
		// without parsing the message body.
		headers := map[string]string{
			"event_id":       pgUUIDToString(r.ID),
			"event_type":     r.EventType,
			"aggregate_type": r.AggregateType,
			"aggregate_id":   r.AggregateID,
		}

		// Publish to Kafka using aggregate_id as the message key.
		// This ensures all events for the same aggregate land on the same partition,
		// preserving per-aggregate ordering.
		err := p.Producer.Publish(r.AggregateID, r.Payload, headers)
		if err != nil {
			_ = p.fail(ctx, id, r.Attempts, err.Error())
			log.Printf("[outbox] publish failed eventId=%s err=%v", pgUUIDToString(r.ID), err)
			continue
		}

		// Mark the event as successfully delivered.
		_ = p.Q.MarkOutboxSent(ctx, r.ID)
	}
}

// claim selects and locks a batch of unsent outbox events within a short
// transaction. It uses FOR UPDATE SKIP LOCKED so multiple publisher instances
// can run concurrently without processing the same event twice.
func (p *Publisher) claim(ctx context.Context) ([]sqlc.ClaimOutboxBatchRow, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	// Rollback is a no-op if the tx was already committed.
	defer func() { _ = tx.Rollback(ctx) }()

	qtx := p.Q.WithTx(tx)

	// SELECT ... FOR UPDATE SKIP LOCKED: atomically claims rows that no other
	// worker has locked, ordered by created_at for FIFO processing.
	rows, err := qtx.ClaimOutboxBatch(ctx, int32(p.BatchSize))
	if err != nil {
		return nil, err
	}

	// Stamp locked_by with this worker's ID for observability —
	// makes it easy to see which worker claimed which events in the DB.
	for _, r := range rows {
		if err := qtx.LockOutboxEvent(ctx, sqlc.LockOutboxEventParams{
			ID:       r.ID,
			LockedBy: pgtype.Text{String: p.WorkerID, Valid: true},
		}); err != nil {
			return nil, err
		}
	}
	return rows, tx.Commit(ctx)
}

// fail records a delivery failure for an outbox event. It increments the
// attempt counter and schedules the next retry using exponential backoff
// (2^attempts seconds, capped at 60s). If max attempts is reached, the
// event is marked DEAD for manual investigation.
func (p *Publisher) fail(ctx context.Context, id [16]byte, attempts int32, msg string) error {
	nextAttempts := int(attempts) + 1

	status := "NEW" // keep retrying
	if nextAttempts >= p.MaxAttempts {
		status = "DEAD" // give up — requires manual intervention
	}

	// Exponential backoff: 2^1=2s, 2^2=4s, 2^3=8s, ..., capped at 60s.
	secs := math.Min(math.Pow(2, float64(nextAttempts)), 60)
	next := time.Now().Add(time.Duration(secs) * time.Second)

	return p.Q.MarkOutboxFailed(ctx, sqlc.MarkOutboxFailedParams{
		ID:            pgtype.UUID{Bytes: id, Valid: true},
		Status:        status,
		NextAttemptAt: pgtype.Timestamptz{Time: next, Valid: true},
		LastError:     pgtype.Text{String: msg, Valid: true},
	})
}

// pgUUIDToString converts a pgtype.UUID to its standard string representation
// (e.g. "550e8400-e29b-41d4-a716-446655440000").
func pgUUIDToString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	b := u.Bytes
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
