package kafka_adapter_producer

import (
	"kafka_module_1/internal/app/config"
	schema_registry "kafka_module_1/internal/pkg/schema-registry"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
)

type KafkaProducer struct {
	producer   *kafka.Producer
	Serializer *jsonschema.Serializer
}

func New() (*KafkaProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.App.GetBootstrapServers(),
		"acks":              "all",
		"retries":           3},
	//  "linger.ms": 0, "batch.num.messages": 1
	)
	if err != nil {
		return nil, err
	}

	s, err := schema_registry.NewSerializer()
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{
		producer:   p,
		Serializer: s,
	}, nil
}
