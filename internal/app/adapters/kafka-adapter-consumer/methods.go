package kafka_adapter_consumer

import "context"

func (a *KafkaConsumer) Start(ctx context.Context) error {
	a.kafkaQueue.Consume(a.consumers.HandleMessage)
	return nil
}
