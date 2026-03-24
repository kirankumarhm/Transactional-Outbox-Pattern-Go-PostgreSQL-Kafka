// Package http contains the Gin HTTP handlers for the orders API.
// The CreateOrder handler implements the write side of the transactional
// outbox pattern: it inserts both the business row and the outbox event
// in a single database transaction.
package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"transactional-outbox/internal/db/sqlc"
	"transactional-outbox/internal/domain"
)

// OrdersHandler handles order-related HTTP requests.
// It holds a connection pool for transaction management and a Queries
// instance for type-safe SQL execution.
type OrdersHandler struct {
	Pool *pgxpool.Pool
	Q    *sqlc.Queries
}

// createOrderReq is the expected JSON body for POST /orders.
type createOrderReq struct {
	CustomerID string  `json:"customerId" binding:"required"`
	Amount     float64 `json:"amount" binding:"required,gt=0"`
}

// Register mounts the order routes onto the Gin engine.
func (h *OrdersHandler) Register(r *gin.Engine) {
	r.POST("/orders", h.CreateOrder)
	r.GET("/health", func(c *gin.Context) { c.String(200, "ok") })
}

// CreateOrder handles POST /orders. It performs two inserts inside a single
// database transaction (the core of the outbox pattern):
//  1. Insert the order row into the orders table.
//  2. Insert the corresponding event into the outbox_events table.
//
// If either insert fails, the entire transaction rolls back — guaranteeing
// that an order never exists without its outbox event, and vice versa.
func (h *OrdersHandler) CreateOrder(c *gin.Context) {
	var req createOrderReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	orderID := uuid.New()

	// Build the domain event that will be written to the outbox table.
	evt := domain.NewOrderCreatedEvent(orderID.String(), req.CustomerID, req.Amount)

	// Execute both inserts within a single transaction.
	err := h.withTx(c.Request.Context(), func(ctx context.Context, qtx *sqlc.Queries) error {
		// Convert uuid.UUID → pgtype.UUID for the sqlc-generated query.
		pgOrderID := pgtype.UUID{Bytes: orderID, Valid: true}

		// pgtype.Numeric requires scanning from a string representation,
		// not a float64 directly — format to 2 decimal places for NUMERIC(12,2).
		var amount pgtype.Numeric
		if err := amount.Scan(fmt.Sprintf("%.2f", req.Amount)); err != nil {
			return fmt.Errorf("scan amount: %w", err)
		}

		// Step 1: Insert the business row.
		if err := qtx.CreateOrder(ctx, sqlc.CreateOrderParams{
			ID:         pgOrderID,
			CustomerID: req.CustomerID,
			Amount:     amount,
			Status:     "CREATED",
		}); err != nil {
			return err
		}

		// Marshal the event payload to JSON for the outbox JSONB column.
		b, err := json.Marshal(evt.Payload)
		if err != nil {
			return err
		}

		// Step 2: Insert the outbox event in the SAME transaction.
		// The background publisher will pick this up and relay it to Kafka.
		evtUUID := pgtype.UUID{Bytes: evt.ID, Valid: true}
		return qtx.InsertOutboxEvent(ctx, sqlc.InsertOutboxEventParams{
			ID:            evtUUID,
			AggregateType: evt.AggregateType,
			AggregateID:   evt.AggregateID,
			EventType:     evt.EventType,
			Payload:       b,
		})
	})

	if err != nil {
		log.Printf("[orders] create failed: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create order"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"orderId": orderID.String(),
		"status":  "CREATED",
	})
}

// withTx executes fn inside a database transaction. If fn returns an error
// the transaction is rolled back; otherwise it is committed.
// This ensures atomicity of the order + outbox event writes.
func (h *OrdersHandler) withTx(ctx context.Context, fn func(context.Context, *sqlc.Queries) error) error {
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	// Rollback is a no-op if the tx was already committed.
	defer func() { _ = tx.Rollback(ctx) }()

	// Bind the Queries instance to this transaction so all SQL runs within it.
	qtx := h.Q.WithTx(tx)
	if err := fn(ctx, qtx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
