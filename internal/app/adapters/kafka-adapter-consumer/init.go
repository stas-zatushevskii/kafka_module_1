package kafka_adapter_consumer

import (
	"kafka_module_1/internal/app/adapters/kafka-adapter-consumer/consumers"
	"kafka_module_1/internal/app/adapters/kafka-adapter-consumer/queue"
	jsonSchema "kafka_module_1/internal/pkg/schema-registry"
)

type consumerMode string

var (
	ConsumerModeSingle consumerMode = "single"
	ConsumerModeBatch  consumerMode = "kafka"
)

type KafkaConsumer struct {
	kafkaQueue *queue.KafkaQueue
	consumers  *consumers.MyConsumer
	mode       consumerMode
	groupID    int
	topic      string
}

type ConsumerBuilder struct {
	consumer KafkaConsumer
}

// NewBuilder inspiration: https://medium.com/@perederei/the-ultimate-guide-for-the-builder-pattern-in-go-6f65e2ecc0a6
func NewBuilder() ConsumerBuilder {
	return ConsumerBuilder{}
}

func (b ConsumerBuilder) SetMode(mode consumerMode) ConsumerBuilder {
	b.consumer.mode = mode
	return b
}

func (b ConsumerBuilder) SetGroupID(groupID int) ConsumerBuilder {
	b.consumer.groupID = groupID
	return b
}

func (b ConsumerBuilder) SetTopic(topic string) ConsumerBuilder {
	b.consumer.topic = topic
	return b
}

func (b ConsumerBuilder) Build() (*KafkaConsumer, error) {
	kafkaQueue, err := queue.New(b.consumer.topic, b.consumer.groupID)
	if err != nil {
		return nil, err
	}
	b.consumer.kafkaQueue = kafkaQueue

	d, err := jsonSchema.NewDeserializer()
	if err != nil {
		return nil, err
	}

	b.consumer.consumers = consumers.New(d)

	return &b.consumer, nil
}
