package kafka_adapter_consumer

import (
	"context"
	"errors"
)

func (a *KafkaConsumer) Start(ctx context.Context) error {
	switch a.mode {
	case ConsumerModeSingle:
		return a.kafkaQueue.ConsumeSingleMode(ctx, a.consumers.HandleSingleMessage)
	case ConsumerModeBatch:
		return a.kafkaQueue.ConsumeBatchMode(ctx, a.consumers.HandleBatchMessages)
	default:
		return errors.New("consumer Mode must be one of: single, multi")
	}
}
