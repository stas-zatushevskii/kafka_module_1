package schema_registry

import (
	"fmt"
	"kafka_module_1/internal/app/config"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
)

func NewSerializer() (*jsonschema.Serializer, error) {
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(config.App.GetSchemeRegistryURL()))
	if err != nil {
		return nil, fmt.Errorf("error creating schema registry client: %w", err)
	}

	serializer, err := jsonschema.NewSerializer(srClient, serde.ValueSerde, jsonschema.NewSerializerConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create json schema serializer: %v", err)
	}

	return serializer, nil
}

func NewDeserializer() (*jsonschema.Deserializer, error) {
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(config.App.GetSchemeRegistryURL()))
	if err != nil {
		return nil, fmt.Errorf("error creating schema registry client: %w", err)
	}

	deserializer, err := jsonschema.NewDeserializer(srClient, serde.ValueSerde, jsonschema.NewDeserializerConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create json schema deserializer: %v", err)
	}

	return deserializer, nil
}
