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

type Publisher struct {
	Pool        *pgxpool.Pool
	Q           *sqlc.Queries
	Producer    *KafkaProducer
	WorkerID    string
	BatchSize   int
	Interval    time.Duration
	MaxAttempts int
}

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

func (p *Publisher) tick(ctx context.Context) {
	rows, err := p.claim(ctx)
	if err != nil {
		log.Printf("[outbox] claim error: %v", err)
		return
	}
	for _, r := range rows {
		id := r.ID.Bytes
		headers := map[string]string{
			"event_id":       pgUUIDToString(r.ID),
			"event_type":     r.EventType,
			"aggregate_type": r.AggregateType,
			"aggregate_id":   r.AggregateID,
		}

		err := p.Producer.Publish(r.AggregateID, r.Payload, headers)
		if err != nil {
			_ = p.fail(ctx, id, r.Attempts, err.Error())
			log.Printf("[outbox] publish failed eventId=%s err=%v", pgUUIDToString(r.ID), err)
			continue
		}

		_ = p.Q.MarkOutboxSent(ctx, r.ID)
	}
}

func (p *Publisher) claim(ctx context.Context) ([]sqlc.ClaimOutboxBatchRow, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	qtx := p.Q.WithTx(tx)
	rows, err := qtx.ClaimOutboxBatch(ctx, int32(p.BatchSize))
	if err != nil {
		return nil, err
	}

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

func (p *Publisher) fail(ctx context.Context, id [16]byte, attempts int32, msg string) error {
	nextAttempts := int(attempts) + 1

	status := "NEW"
	if nextAttempts >= p.MaxAttempts {
		status = "DEAD"
	}

	secs := math.Min(math.Pow(2, float64(nextAttempts)), 60)
	next := time.Now().Add(time.Duration(secs) * time.Second)

	return p.Q.MarkOutboxFailed(ctx, sqlc.MarkOutboxFailedParams{
		ID:            pgtype.UUID{Bytes: id, Valid: true},
		Status:        status,
		NextAttemptAt: pgtype.Timestamptz{Time: next, Valid: true},
		LastError:     pgtype.Text{String: msg, Valid: true},
	})
}

func pgUUIDToString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	b := u.Bytes
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
