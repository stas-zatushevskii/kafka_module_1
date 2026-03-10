package kafka_adapter_consumer

import (
	"context"
	"errors"
	"kafka_module_1/internal/app/config"
)

func (a *KafkaConsumer) Start(ctx context.Context) error {
	switch config.App.GetConsumerMode() {
	case config.SingleMode:
		return a.kafkaQueue.ConsumeSingleMode(ctx, a.consumers.HandleSingleMessage)
	case config.BatchMode:
		return a.kafkaQueue.ConsumeBatchMode(ctx, a.consumers.HandleBatchMessages)
	default:
		return errors.New("consumer Mode must be one of: single, multi")
	}
}
