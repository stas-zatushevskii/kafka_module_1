package app

import (
	"context"
	"fmt"
	kafka_adapter_consumer "kafka_module_1/internal/app/adapters/kafka-adapter-consumer"
	kafka_adapter_producer "kafka_module_1/internal/app/adapters/kafka-adapter-producer"
	infinite_producer "kafka_module_1/internal/app/adapters/kafka-adapter-producer/producers"
	os_signal_adapter "kafka_module_1/internal/app/adapters/os-signal-adapter"
	"kafka_module_1/internal/app/config"
	"kafka_module_1/internal/pkg/graceful"
)

type App struct {
	KafkaProducer                *kafka_adapter_producer.KafkaProducer
	KafkaConsumer                *kafka_adapter_consumer.KafkaConsumer
	OSSignalAdapter              *os_signal_adapter.OsSignalAdapter
	KafkaInfiniteMessageProducer *infinite_producer.MyProducer
}

// New initializes the application by loading configuration,
// creating OS signal, Kafka producer/consumer adapters,
// starting the infinite Kafka message producer loop,
// and returning the assembled App instance.
func New() (*App, error) {
	// load config
	if err := config.GetConfig(); err != nil {
		return nil, fmt.Errorf("load config failed: %v", err)
	}

	// os signal adapter
	osSignalAdapter := os_signal_adapter.New()

	// kafka producer adapter
	kafkaProducerAdapter, err := kafka_adapter_producer.New(config.App.GetTopicName())
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer failed: %v", err)
	}

	// kafka consumer adapter
	kafkaConsumerAdapter, err := kafka_adapter_consumer.New()
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer failed: %v", err)
	}

	// infinite loop for message flow in Kafka
	infinite_producer.New(config.App.GetTopicName(), kafkaProducerAdapter)

	app := &App{
		KafkaProducer:   kafkaProducerAdapter,
		KafkaConsumer:   kafkaConsumerAdapter,
		OSSignalAdapter: osSignalAdapter,
	}

	return app, nil
}

// Start runs the application processes under a graceful shutdown supervisor.
func (app *App) Start() error {
	gr := graceful.New(
		graceful.NewProcess(app.OSSignalAdapter),
		graceful.NewProcess(app.KafkaInfiniteMessageProducer),
		graceful.NewProcess(app.KafkaConsumer),
	)

	err := gr.Start(context.Background())
	if err != nil {
		return err
	}

	return nil
}
