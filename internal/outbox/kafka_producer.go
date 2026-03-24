package outbox

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"transactional-outbox/internal/config"
)

type KafkaProducer struct {
	p          *kafka.Producer
	topic      string
	flushTimeout int
}

func NewKafkaProducer(cfg config.KafkaConfig) (*KafkaProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        cfg.Brokers,
		"enable.idempotence":       true,
		"acks":                     "all",
		"retries":                  cfg.Retries,
		"linger.ms":                cfg.LingerMs,
		"batch.num.messages":       cfg.BatchNumMessages,
		"compression.type":         cfg.CompressionType,
		"message.send.max.retries": cfg.MaxSendRetries,
	})
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{p: p, topic: cfg.Topic, flushTimeout: cfg.FlushTimeoutMs}, nil
}

func (k *KafkaProducer) Close() {
	k.p.Flush(k.flushTimeout)
	k.p.Close()
}

func (k *KafkaProducer) Publish(key string, value []byte, headers map[string]string) error {
	var hdrs []kafka.Header
	for hk, hv := range headers {
		hdrs = append(hdrs, kafka.Header{Key: hk, Value: []byte(hv)})
	}

	delivery := make(chan kafka.Event, 1)
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

	e := <-delivery
	m := e.(*kafka.Message)
	close(delivery)

	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}
	return nil
}
