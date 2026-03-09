package kafka_adapter_consumer

import "context"

func (a *KafkaConsumer) Start(ctx context.Context) error {
	return a.kafkaQueue.Consume(ctx, a.consumers.HandleMessage)
}
