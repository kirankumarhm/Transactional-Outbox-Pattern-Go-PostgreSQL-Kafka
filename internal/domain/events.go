package domain

import "github.com/google/uuid"

type OutboxEvent struct {
    ID            uuid.UUID
    AggregateType string
    AggregateID   string
    EventType     string
    Payload       any
}

type OrderCreatedPayload struct {
    OrderID    string  `json:"orderId"`
    CustomerID string  `json:"customerId"`
    Amount     float64 `json:"amount"`
}

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
