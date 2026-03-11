package consumers

import "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"

type MyConsumer struct {
	topicName    string
	deserializer *jsonschema.Deserializer
}

func New(deserializer *jsonschema.Deserializer) *MyConsumer {
	return &MyConsumer{deserializer: deserializer}
}
