package config

import "time"

type AppConfig struct {
	kafkaConsumer  kafkaConsumer
	kafkaProducer  kafkaProducer
	kafkaAdmin     kafkaAdmin
	schemaRegistry schemaRegistry
}

type kafkaProducer struct {
	ack         string
	loopTimeout time.Duration
}

type kafkaConsumer struct {
	bootstrapServers string
	consumerGroupID  int
	topicName        string
}

type kafkaAdmin struct {
	fetchMinBytes int
	fetchMaxMs    int
}

type schemaRegistry struct {
	url string
}
