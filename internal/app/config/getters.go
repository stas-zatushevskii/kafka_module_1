package config

import "time"

func (a *AppConfig) GetSingleModeConsumerGroupID() int {
	return a.kafkaConsumer.singleModeConsumerGroupID
}

func (a *AppConfig) GetBatchModeConsumerGroupID() int {
	return a.kafkaConsumer.batchModeConsumerGroupID
}

func (a *AppConfig) GetTopicName() string {
	return a.kafkaConsumer.topicName
}

func (a *AppConfig) GetBootstrapServers() string {
	return a.kafkaConsumer.bootstrapServers
}

func (a *AppConfig) GetFetchMinBytes() int {
	return a.kafkaAdmin.fetchMinBytes
}

func (a *AppConfig) GetFetchMaxMs() int {
	return a.kafkaAdmin.fetchMaxMs
}

func (a *AppConfig) GetAck() string {
	return a.kafkaProducer.ack
}

func (a *AppConfig) GetProducerFlushInterval() time.Duration {
	return a.kafkaProducer.flushInterval
}

func (a *AppConfig) GetConsumerMode() ConsumerMode {
	return a.kafkaConsumer.mode
}

func (a *AppConfig) GetSchemeRegistryURL() string {
	return a.schemaRegistry.url
}
