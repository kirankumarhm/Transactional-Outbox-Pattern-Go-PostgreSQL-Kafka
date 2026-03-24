package outbox

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"transactional-outbox/internal/config"
)

// KafkaProducer wraps the confluent-kafka-go producer with a fixed topic
// and synchronous delivery semantics. It is configured with idempotent
// production (enable.idempotence=true) and acks=all for exactly-once
// delivery guarantees at the producer level.
type KafkaProducer struct {
	p            *kafka.Producer // underlying confluent-kafka-go producer
	topic        string          // target Kafka topic for all messages
	flushTimeout int             // milliseconds to wait for in-flight messages on Close
}

// NewKafkaProducer creates a new idempotent Kafka producer from the provided config.
// Key producer settings:
//   - enable.idempotence=true: prevents duplicate messages on retries
//   - acks=all: waits for all in-sync replicas to acknowledge
//   - compression.type: reduces network bandwidth (default: snappy)
func NewKafkaProducer(cfg config.KafkaConfig) (*KafkaProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        cfg.Brokers,
		"enable.idempotence":       true,  // exactly-once semantics at the producer level
		"acks":                     "all", // wait for all ISR replicas to acknowledge
		"retries":                  cfg.Retries,
		"linger.ms":                cfg.LingerMs,          // batch delay for throughput
		"batch.num.messages":       cfg.BatchNumMessages,  // max messages per batch
		"compression.type":         cfg.CompressionType,   // snappy, lz4, zstd, etc.
		"message.send.max.retries": cfg.MaxSendRetries,
	})
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{p: p, topic: cfg.Topic, flushTimeout: cfg.FlushTimeoutMs}, nil
}

// Close flushes any pending messages (waiting up to flushTimeout ms)
// and then shuts down the underlying producer.
func (k *KafkaProducer) Close() {
	k.p.Flush(k.flushTimeout)
	k.p.Close()
}

// Publish sends a single message to Kafka synchronously. It blocks until
// the broker acknowledges delivery or returns an error.
//
// Parameters:
//   - key: used for partition assignment (aggregate_id ensures per-aggregate ordering)
//   - value: the message body (JSON-encoded outbox event payload)
//   - headers: metadata attached to the Kafka message (event_id, event_type, etc.)
func (k *KafkaProducer) Publish(key string, value []byte, headers map[string]string) error {
	var hdrs []kafka.Header
	for hk, hv := range headers {
		hdrs = append(hdrs, kafka.Header{Key: hk, Value: []byte(hv)})
	}

	// Buffered channel of size 1 to receive the delivery report.
	delivery := make(chan kafka.Event, 1)

	// Produce enqueues the message; delivery confirmation arrives on the channel.
	err := k.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &k.topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          value,
		Headers:        hdrs,
		Timestamp:      time.Now(),
	}, delivery)

	if err != nil {
		return err
	}

	// Block until the broker confirms delivery.
	e := <-delivery
	m := e.(*kafka.Message)
	close(delivery)

	// Check if the broker reported a delivery error.
	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}
	return nil
}
