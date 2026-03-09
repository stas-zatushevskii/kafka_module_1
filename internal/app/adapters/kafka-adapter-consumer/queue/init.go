package queue

import (
	"fmt"
	"kafka_module_1/internal/app/config"
	worker_pool "kafka_module_1/internal/pkg/worker-pool"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaQueue struct {
	kafkaConsumer *kafka.Consumer
	partitions    map[PartitionKey][]*kafka.Message
	wp            *worker_pool.WorkerPool
}

func New(topic string) (*KafkaQueue, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  config.App.GetBootstrapServers(),
		"group.id":           config.App.GetConsumerGroupID(),
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
