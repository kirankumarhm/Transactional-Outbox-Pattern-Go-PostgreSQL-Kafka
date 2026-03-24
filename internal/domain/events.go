// Package domain defines the core business event types used by the
// transactional outbox pattern. Events are created in the HTTP handler
// and persisted to the outbox_events table within the same DB transaction
// as the business data.
package domain

import "github.com/google/uuid"

// OutboxEvent represents a domain event to be written into the outbox table.
// The Payload field is serialized to JSON before storage.
type OutboxEvent struct {
	ID            uuid.UUID // unique event identifier
	AggregateType string    // entity type, e.g. "order"
	AggregateID   string    // entity ID, used as the Kafka message key for ordering
	EventType     string    // event name, e.g. "OrderCreated"
	Payload       any       // event body, marshalled to JSON
}

// OrderCreatedPayload is the JSON payload for an OrderCreated event.
type OrderCreatedPayload struct {
	OrderID    string  `json:"orderId"`
	CustomerID string  `json:"customerId"`
	Amount     float64 `json:"amount"`
}

// NewOrderCreatedEvent constructs an OutboxEvent for a newly created order.
// It generates a fresh UUID for the event ID and sets the aggregate key
// to the order ID so all events for the same order land on the same Kafka partition.
func NewOrderCreatedEvent(orderID, customerID string, amount float64) OutboxEvent {
	return OutboxEvent{
		ID:            uuid.New(),
		AggregateType: "order",
		AggregateID:   orderID,
		EventType:     "OrderCreated",
		Payload: OrderCreatedPayload{
			OrderID:    orderID,
			CustomerID: customerID,
			Amount:     amount,
		},
	}
}
