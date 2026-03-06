package kafka_adapter_consumer

import (
	"kafka_module_1/internal/app/adapters/kafka-adapter-consumer/consumers"
	"kafka_module_1/internal/app/adapters/kafka-adapter-consumer/queue"
	"kafka_module_1/internal/app/config"
	jsonSchema "kafka_module_1/internal/pkg/schema-registry"
)

type KafkaConsumer struct {
	kafkaQueue *queue.KafkaQueue
	consumers  *consumers.MyConsumer
}

func New() (*KafkaConsumer, error) {
	kafkaQueue, err := queue.New(config.App.GetTopicName())
	if err != nil {
		return nil, err
	}

	d, err := jsonSchema.NewDeserializer()
	if err != nil {
		return nil, err
	}

	kafkaHandlers := consumers.New(d)

	return &KafkaConsumer{
		kafkaQueue: kafkaQueue,
		consumers:  kafkaHandlers,
	}, nil
}
