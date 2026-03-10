package queue

import (
	"context"
	"errors"
	"kafka_module_1/internal/pkg/logger"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

var (
	CommitFailed = errors.New("commit failed")
)

// Consume continuously polls and processes Kafka messages using the provided handler;
// stops only on commit failure, otherwise logs errors and keeps running.
func (queue *KafkaQueue) Consume(businessLogicFunc func(context.Context, []byte) error) {
	for {
		err := queue.ProcessOneMessage(businessLogicFunc)
		if err != nil {
			if errors.Is(err, CommitFailed) {
				logger.Log.Error("Commit failed, consumer stopped", zap.Error(err))
				return
			}

			logger.Log.Error(err.Error())
			continue
		}
	}
}

// ProcessOneMessage polls once, invokes the handler for a single Kafka message,
// and commits the offset on success (returns CommitFailed on commit error).
func (queue *KafkaQueue) ProcessOneMessage(businessLogicFunc func(context.Context, []byte) error) error {
	ctx := context.TODO()

	ev := queue.kafkaConsumer.Poll(100) // 100 ms
	if ev == nil {
		return nil
	}

	switch e := ev.(type) {
	case *kafka.Message:
		err := businessLogicFunc(ctx, e.Value)
		if err != nil {
			return err
		}
		if _, err := queue.kafkaConsumer.Commit(); err != nil {
			return CommitFailed
		}
	case kafka.Error:
		return e
	}

	return nil
}
