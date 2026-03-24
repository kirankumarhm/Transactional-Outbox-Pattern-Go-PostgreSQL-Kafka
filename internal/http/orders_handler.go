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

type OrdersHandler struct {
	Pool *pgxpool.Pool
	Q    *sqlc.Queries
}

type createOrderReq struct {
	CustomerID string  `json:"customerId" binding:"required"`
	Amount     float64 `json:"amount" binding:"required,gt=0"`
}

func (h *OrdersHandler) Register(r *gin.Engine) {
	r.POST("/orders", h.CreateOrder)
	r.GET("/health", func(c *gin.Context) { c.String(200, "ok") })
}

func (h *OrdersHandler) CreateOrder(c *gin.Context) {
	var req createOrderReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	orderID := uuid.New()
	evt := domain.NewOrderCreatedEvent(orderID.String(), req.CustomerID, req.Amount)

	err := h.withTx(c.Request.Context(), func(ctx context.Context, qtx *sqlc.Queries) error {
		pgOrderID := pgtype.UUID{Bytes: orderID, Valid: true}

		var amount pgtype.Numeric
		if err := amount.Scan(fmt.Sprintf("%.2f", req.Amount)); err != nil {
			return fmt.Errorf("scan amount: %w", err)
		}

		if err := qtx.CreateOrder(ctx, sqlc.CreateOrderParams{
			ID:         pgOrderID,
			CustomerID: req.CustomerID,
			Amount:     amount,
			Status:     "CREATED",
		}); err != nil {
			return err
		}

		b, err := json.Marshal(evt.Payload)
		if err != nil {
			return err
		}

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

func (h *OrdersHandler) withTx(ctx context.Context, fn func(context.Context, *sqlc.Queries) error) error {
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	qtx := h.Q.WithTx(tx)
	if err := fn(ctx, qtx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
