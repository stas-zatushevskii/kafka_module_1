package kafka_adapter_producer

import (
	schema_registry "kafka_module_1/internal/pkg/schema-registry"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
)

type KafkaProducer struct {
	producer         *kafka.Producer
	acks             string
	bootstrapServers string
	Serializer       *jsonschema.Serializer
}

type KafkaProducerBuilder struct {
	producer KafkaProducer
}

func NewBuilder() KafkaProducerBuilder {
	return KafkaProducerBuilder{}
}

func (b KafkaProducerBuilder) SetBootstrapServers(bootstrapServers string) KafkaProducerBuilder {
	b.producer.bootstrapServers = bootstrapServers
	return b
}

func (b KafkaProducerBuilder) SetAcks(acks string) KafkaProducerBuilder {
	b.producer.acks = acks
	return b
}

func (b KafkaProducerBuilder) Build() (*KafkaProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": b.producer.bootstrapServers,
		"acks":              b.producer.acks,
		"retries":           3},
	)
	if err != nil {
		return nil, err
	}
	b.producer.producer = p

	s, err := schema_registry.NewSerializer()
	if err != nil {
		return nil, err
	}

	b.producer.Serializer = s

	return &b.producer, nil
}
