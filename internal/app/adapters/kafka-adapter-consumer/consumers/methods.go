package consumers

import (
	"context"
	"fmt"
	"kafka_module_1/internal/pkg/logger"
	"strings"

	"kafka_module_1/internal/app/domain"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// HandleSingleMessage handle message:
// 1. deserialization
// 2. business logic
func (a *MyConsumer) HandleSingleMessage(ctx context.Context, message *kafka.Message) error {
	var msg domain.KafkaMessage

	if err := a.deserializer.DeserializeInto(a.topicName, message.Value, &msg); err != nil {
		return err
	}

	// here will be placed service layer call in real project...
	logger.Log.Info(fmt.Sprintf("single mode: got new message UserID: %d; partition: %d", msg.UserID, message.TopicPartition.Partition))

	return nil
}

func (a *MyConsumer) HandleBatchMessages(ctx context.Context, messages []*kafka.Message) error {
	var (
		msgs []domain.KafkaMessage
		info strings.Builder
	)
	for _, message := range messages {
		var msg domain.KafkaMessage
		if err := a.deserializer.DeserializeInto(a.topicName, message.Value, &msg); err != nil {
			return err
		}
		msgs = append(msgs, msg)
	}
	for i, msg := range msgs {
		info.WriteString(fmt.Sprintf("userID: %d; partition: %d, ", msg.UserID, messages[i].TopicPartition.Partition))
	}

	logger.Log.Info(fmt.Sprintf("got %d messages: %s from partition", len(msgs), info.String()))

	return nil
}
