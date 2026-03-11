package config

import "time"

type ConsumerMode string

type AppConfig struct {
	kafkaConsumer  kafkaConsumer
	kafkaProducer  kafkaProducer
	kafkaAdmin     kafkaAdmin
	schemaRegistry schemaRegistry
}

type kafkaProducer struct {
	ack           string
	flushInterval time.Duration
}

type kafkaConsumer struct {
	bootstrapServers          string
	singleModeConsumerGroupID int
	batchModeConsumerGroupID  int
	topicName                 string
	mode                      ConsumerMode
}

type kafkaAdmin struct {
	fetchMinBytes int
	fetchMaxMs    int
}

type schemaRegistry struct {
	url string
}
