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
	KafkaConsumerSingleMode      *kafka_adapter_consumer.KafkaConsumer
	KafkaConsumerBatchMode       *kafka_adapter_consumer.KafkaConsumer
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
	kafkaProducerAdapter, err := kafka_adapter_producer.NewBuilder().
		SetBootstrapServers(config.App.GetBootstrapServers()).
		SetAcks(config.App.GetAck()).
		Build()
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer failed: %v", err)
	}

	// kafka consumer adapter (running in single mode)
	ConsumerSingle, err := kafka_adapter_consumer.NewBuilder().
		SetMode(kafka_adapter_consumer.ConsumerModeSingle).
		SetGroupID(config.App.GetSingleModeConsumerGroupID()).
		SetTopic(config.App.GetTopicName()).
		Build()
	if err != nil {
		return nil, fmt.Errorf("build Kafka single mode consumer failed: %v", err)
	}

	// kafka consumer adapter (running in batch mode)
	ConsumerBatch, err := kafka_adapter_consumer.NewBuilder().
		SetMode(kafka_adapter_consumer.ConsumerModeBatch).
		SetGroupID(config.App.GetBatchModeConsumerGroupID()).
		SetTopic(config.App.GetTopicName()).
		Build()
	if err != nil {
		return nil, fmt.Errorf("build Kafka batch mode consumer failed: %v", err)
	}

	// infinite loop for message flow in Kafka
	kafkaProducer := infinite_producer.New(config.App.GetTopicName(), kafkaProducerAdapter)

	app := &App{
		KafkaInfiniteMessageProducer: kafkaProducer,
		KafkaConsumerSingleMode:      ConsumerSingle,
		KafkaConsumerBatchMode:       ConsumerBatch,
		OSSignalAdapter:              osSignalAdapter,
	}

	return app, nil
}

// Start runs the application processes under a graceful shutdown supervisor.
func (app *App) Start() error {
	gr := graceful.New(
		graceful.NewProcess(app.OSSignalAdapter),
		graceful.NewProcess(app.KafkaInfiniteMessageProducer),
		graceful.NewProcess(app.KafkaConsumerSingleMode),
		graceful.NewProcess(app.KafkaConsumerBatchMode),
	)

	err := gr.Start(context.Background())
	if err != nil {
		return err
	}

	return nil
}
