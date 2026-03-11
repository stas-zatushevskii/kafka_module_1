package config

import (
	"errors"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/joho/godotenv"
)

var (
	App       *AppConfig
	initError error
	once      sync.Once
)

// GetConfig loads singleton config (file -> env -> flags)
func GetConfig() error {
	once.Do(func() {
		cfg := &AppConfig{}

		if err := applyENV(cfg); err != nil {
			initError = err
		}

		App = cfg
	})

	return initError
}

func applyENV(cfg *AppConfig) error {
	err := godotenv.Load()
	if err != nil {
		log.Println("no .env file found")
	}

	if consumerGroupID, ok := os.LookupEnv("SINGLE_MODE_CONSUMER_GROUP_ID"); ok {
		id, err := strconv.Atoi(consumerGroupID)
		if err != nil || id <= 0 {
			return errors.New("SINGLE_MODE_CONSUMER_GROUP_ID must be a positive integer")
		}
		cfg.kafkaConsumer.singleModeConsumerGroupID = id
	} else {
		return errors.New("SINGLE_MODE_CONSUMER_GROUP_ID must be set")
	}

	if consumerGroupID, ok := os.LookupEnv("BATCH_MODE_CONSUMER_GROUP_ID"); ok {
		id, err := strconv.Atoi(consumerGroupID)
		if err != nil || id <= 0 {
			return errors.New("BATCH_MODE_CONSUMER_GROUP_ID must be a positive integer")
		}
		cfg.kafkaConsumer.batchModeConsumerGroupID = id
	} else {
		return errors.New("BATCH_MODE_CONSUMER_GROUP_ID must be set")
	}

	if topic, ok := os.LookupEnv("TOPIC_NAME"); ok {
		if topic == "" {
			return errors.New("TOPIC environment variable must be set")
		}
		cfg.kafkaConsumer.topicName = topic
	} else {
		return errors.New("TOPIC_NAME environment variable must be set")
	}

	if bootstrapServer, ok := os.LookupEnv("BOOTSTRAP_SERVER"); ok {
		cfg.kafkaConsumer.bootstrapServers = bootstrapServer
	} else {
		return errors.New("BOOTSTRAP_SERVER environment variable must be set")
	}

	if ack, ok := os.LookupEnv("ACK_MESSAGE"); ok {
		if ack != "0" && ack != "1" && ack != "all" && ack != "-1" {
			return errors.New("got invalid ack value, possible values: '0', '1', 'all', '-1'")
		}
		cfg.kafkaProducer.ack = ack
	} else {
		return errors.New("ACK_MESSAGE environment variable must be set")
	}

	if url, ok := os.LookupEnv("SCHEMA_REGISTRY_URL"); ok {
		cfg.schemaRegistry.url = url
	} else {
		return errors.New("SCHEMA_REGISTRY_URL environment variable must be set")
	}

	if fetchMinBytes, ok := os.LookupEnv("FETCH_MIN_BYTES"); ok {
		v, err := strconv.Atoi(fetchMinBytes)
		if err != nil {
			return errors.New("FETCH_MIN_BYTES must be a positive integer")
		}
		cfg.kafkaAdmin.fetchMinBytes = v
	} else {
		return errors.New("FETCH_MIN_BYTES environment variable must be set")
	}

	if fetchMaxMs, ok := os.LookupEnv("FETCH_MAX_MS"); ok {
		v, err := strconv.Atoi(fetchMaxMs)
		if err != nil {
			return errors.New("FETCH_MAX_MS must be a positive integer")
		}
		cfg.kafkaAdmin.fetchMaxMs = v
	} else {
		return errors.New("FETCH_MAX_MS environment variable must be set")
	}

	if flushInterval, ok := os.LookupEnv("PRODUCER_FLUSH_INTERVAL_MS"); ok {
		v, err := strconv.Atoi(flushInterval)
		if err != nil {
			return errors.New("PRODUCER_FLUSH_INTERVAL_MS must be a positive integer")
		}
		cfg.kafkaProducer.flushInterval = time.Duration(v)
	} else {
		return errors.New("PRODUCER_FLUSH_INTERVAL_MS environment variable must be set")
	}

	return nil
}
