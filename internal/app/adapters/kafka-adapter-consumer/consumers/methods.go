package consumers

import (
	"context"
	"fmt"
	"kafka_module_1/internal/pkg/logger"
)

type KafkaMessage struct {
	UserID   int
	Username string
	Event    string
}

// HandleMessage handle message:
// 1. deserialization
// 2. business logic
func (a *MyConsumer) HandleMessage(ctx context.Context, message []byte) error {
	var msg KafkaMessage

	if err := a.deserializer.DeserializeInto(a.topicName, message, &msg); err != nil {
		return err
	}

	// here will be placed service layer call in real project...
	logger.Log.Info(fmt.Sprintf("got new message from Kafka: %s", string(message)))

	return nil
}
