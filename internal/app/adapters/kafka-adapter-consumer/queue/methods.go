package queue

import (
	"context"
	"errors"
	"kafka_module_1/internal/pkg/logger"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

var (
	CommitFailed = errors.New("commit failed")
)

type Key struct {
	Topic     string
	Partition int32
}

// ConsumeSingleMode continuously polls and processes Kafka messages using the provided handler;
// stops only on commit failure, otherwise logs errors and keeps running.
func (queue *KafkaQueue) ConsumeSingleMode(ctx context.Context, businessLogicFunc func(context.Context, *kafka.Message) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			err := queue.ProcessOneMessage(ctx, businessLogicFunc)
			if err != nil {
				if errors.Is(err, CommitFailed) {
					logger.Log.Error("Commit failed, consumer stopped", zap.Error(err))
					return err
				}

				logger.Log.Error(err.Error())
				continue
			}
		}
	}
}

// ConsumeBatchMode starts batch consumption mode.
// It collects messages into batches and passes them to the provided business handler.
// Consumption stops on commit failure or any unrecoverable Kafka error.
func (queue *KafkaQueue) ConsumeBatchMode(ctx context.Context, businessLogicFunc func(context.Context, []*kafka.Message) error) error {
	err := queue.ProcessBatchMessages(ctx, 10, 5, businessLogicFunc)
	if err != nil {
		if errors.Is(err, CommitFailed) {
			logger.Log.Error("Commit failed, consumer stopped", zap.Error(err))
			return err
		}

		logger.Log.Error(err.Error())

	}
	return nil
}

// ProcessOneMessage polls once, invokes the handler for a single Kafka message,
// and commits the offset on success (returns CommitFailed on commit error).
func (queue *KafkaQueue) ProcessOneMessage(ctx context.Context, businessLogicFunc func(context.Context, *kafka.Message) error) error {

	ev := queue.kafkaConsumer.Poll(100) // 100 ms
	if ev == nil {
		return nil
	}

	switch e := ev.(type) {
	case *kafka.Message:
		err := businessLogicFunc(ctx, e)
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

// ProcessBatchMessages collects Kafka messages into an in-memory batch
// and flushes it either when the batch size limit is reached or when the
// flush interval expires. After successful processing, it commits offsets
// for the latest message of each topic-partition in the batch.
func (queue *KafkaQueue) ProcessBatchMessages(
	ctx context.Context,
	batchSize int,
	flushInterval time.Duration,
	businessLogicFunc func(context.Context, []*kafka.Message) error,
) error {

	ticker := time.NewTicker(flushInterval * time.Second)
	defer ticker.Stop()

	batch := make([]*kafka.Message, 0, batchSize)

	for {
		select {
		case <-ctx.Done():
			if err := queue.flush(ctx, businessLogicFunc, append([]*kafka.Message(nil), batch...)); err != nil {
				return err
			}
			batch = batch[:0]
			return ctx.Err()

		case <-ticker.C:
			if err := queue.flush(ctx, businessLogicFunc, append([]*kafka.Message(nil), batch...)); err != nil {
				return err
			}
			batch = batch[:0]

		default:
			ev := queue.kafkaConsumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				batch = append(batch, e)

				if len(batch) >= batchSize {
					if err := queue.flush(ctx, businessLogicFunc, append([]*kafka.Message(nil), batch...)); err != nil {
						return err
					}
					batch = batch[:0]
				}

			case kafka.Error:
				return e
			}
		}
	}
}

// flush processes the provided batch with the business handler
// and commits Kafka offsets only after successful processing.
// For each topic-partition, only the latest offset from the batch is committed.
func (queue *KafkaQueue) flush(ctx context.Context, f func(context.Context, []*kafka.Message) error, batch []*kafka.Message) error {
	if len(batch) == 0 {
		return nil
	}

	if err := f(ctx, batch); err != nil {
		return err
	}

	lastOffsets := make(map[Key]kafka.TopicPartition)

	for _, msg := range batch {

		key := Key{
			Topic:     *msg.TopicPartition.Topic,
			Partition: msg.TopicPartition.Partition,
		}

		lastOffsets[key] = kafka.TopicPartition{
			Topic:     msg.TopicPartition.Topic,
			Partition: msg.TopicPartition.Partition,
			Offset:    msg.TopicPartition.Offset + 1,
		}
	}

	offsets := make([]kafka.TopicPartition, 0, len(lastOffsets))
	for _, tp := range lastOffsets {
		offsets = append(offsets, tp)
	}

	if _, err := queue.kafkaConsumer.CommitOffsets(offsets); err != nil {
		return CommitFailed
	}

	return nil

}
