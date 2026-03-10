package consumers

import (
	"context"
	"fmt"
	"kafka_module_1/internal/pkg/logger"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaMessage struct {
	UserID   int
	Username string
	Event    string
}

// HandleSingleMessage handle message:
// 1. deserialization
// 2. business logic
func (a *MyConsumer) HandleSingleMessage(ctx context.Context, message *kafka.Message) error {
	var msg KafkaMessage

	if err := a.deserializer.DeserializeInto(a.topicName, message.Value, &msg); err != nil {
		return err
	}

	// here will be placed service layer call in real project...
	logger.Log.Info(fmt.Sprintf("got new message from Kafka: %d for UserID", msg.UserID))

	return nil
}

func (a *MyConsumer) HandleBatchMessages(ctx context.Context, messages []*kafka.Message) error {
	var (
		msgs []KafkaMessage
		info strings.Builder
	)
	for _, message := range messages {
		var msg KafkaMessage
		if err := a.deserializer.DeserializeInto(a.topicName, message.Value, &msg); err != nil {
			return err
		}
		msgs = append(msgs, msg)
	}
	logger.Log.Info(fmt.Sprintf("got %d messages", len(msgs)))
	for _, msg := range msgs {
		info.WriteString(fmt.Sprintf("got new message from Kafka for UserID: %d", msg.UserID))
	}
	return nil
}
