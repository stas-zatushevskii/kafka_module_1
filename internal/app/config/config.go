package config

import "time"

type ConsumerMode string

var (
	SingleMode ConsumerMode = "s"
	BatchMode  ConsumerMode = "b"
)

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
	bootstrapServers string
	consumerGroupID  int
	topicName        string
	mode             ConsumerMode
}

type kafkaAdmin struct {
	fetchMinBytes int
	fetchMaxMs    int
}

type schemaRegistry struct {
	url string
}
