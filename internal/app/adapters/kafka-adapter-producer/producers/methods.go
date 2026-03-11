package producers

import (
	"context"
	"errors"
	"time"

	"kafka_module_1/internal/app/domain"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	SerializationFailed = errors.New("failed to serialize message")
)

func (p *MyProducer) SendOrderData(ctx context.Context) error {
	userID := time.Now().Second() % 10000

	data := &domain.KafkaMessage{
		UserID:   userID,
		Username: "OYOOYOY",
		Event:    "message",
	}

	payload, err := p.producer.Serializer.Serialize(p.topic, data)
	if err != nil {
		return SerializationFailed
	}

	return p.producer.Produce(ctx, &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload,
	})
}
