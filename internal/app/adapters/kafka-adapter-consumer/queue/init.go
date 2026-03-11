package queue

import (
	"fmt"
	"kafka_module_1/internal/app/config"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaQueue struct {
	kafkaConsumer *kafka.Consumer
}

func New(topic string, groupID int) (*KafkaQueue, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  config.App.GetBootstrapServers(),
		"group.id":           groupID,
		"session.timeout.ms": 6000,
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest"})

	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %v", err.Error())
	}

	err = c.Subscribe(topic, nil)

	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to topics: %v", err.Error())
	}

	return &KafkaQueue{
		kafkaConsumer: c,
	}, err
}
