package kafka_adapter_consumer

import (
	"context"
	"kafka_module_1/internal/app/config"
)

func (a *KafkaConsumer) Start(ctx context.Context) error {
	switch config.App.GetConsumerMode() {
	case config.SingleMode:
		a.kafkaQueue.ConsumeSingleMode(a.consumers.HandleSingleMessage)
	case config.BatchMode:
		a.kafkaQueue.ConsumeBatchMode(ctx, a.consumers.HandleBatchMessages)
	}

	return nil
}
