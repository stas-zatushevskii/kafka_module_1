package queue

import (
	"context"
	"errors"
	"fmt"
	"kafka_module_1/internal/app/config"
	"kafka_module_1/internal/pkg/logger"
	"log"
	"time"

	workerPool "kafka_module_1/internal/pkg/worker-pool"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

var (
	CommitFailed = errors.New("commit failed")
)

type Ack struct {
	Topic     string
	Partition int32
	Offset    kafka.Offset
}

type PartitionKey struct {
	topic     string
	partition int32
}

// Consume continuously polls and processes Kafka messages using the provided handler;
// stops only on commit failure, otherwise logs errors and keeps running.
func (queue *KafkaQueue) Consume(ctx context.Context, businessLogicFunc func(context.Context, []byte) error) error {
	mode := config.App.GetConsumerMode()
	switch mode {
	case config.SingleMode:
		for {
			err := queue.ProcessOneMessage(businessLogicFunc)
			if err != nil {
				if errors.Is(err, CommitFailed) {
					logger.Log.Error("Commit failed, consumer stopped", zap.Error(err))
					return err
				}
				logger.Log.Error(err.Error())
				continue
			}
		}
	case config.BatchMode:
		return queue.ProcessBatchMessages(ctx, businessLogicFunc)
	}
	return nil

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

func (queue *KafkaQueue) ProcessBatchMessages(ctx context.Context, businessLogicFunc func(context.Context, []byte) error) error {
	ticker := time.NewTicker(3 * time.Second)   // fixme: get value from cfg (max wait time for create 1 batch)
	queue.wp = workerPool.NewWorkerPool(ctx, 4) // fixme: get value from cfg
	commitChan := make(chan Ack)

	go func() {
		go func() {
			commitCoordinator(ctx, queue.kafkaConsumer, commitChan)
		}()
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			for k, v := range queue.partitions {
				queue.wp.Set(&workerPool.Task{
					Fn: func(ctx context.Context) error {
						for _, message := range v {
							if err := businessLogicFunc(ctx, message.Value); err != nil {
								return err
							}
							commitChan <- Ack{
								Topic:     k.topic,
								Partition: k.partition,
								Offset:    message.TopicPartition.Offset,
							}
						}
						return nil
					},
				})
				queue.partitions[k] = queue.partitions[k][:] // clear the slice of messages
			}

		default:
			err := queue.kafkaConsumer.Poll(100)
			if err == nil {
				continue
			}
			switch e := err.(type) {
			case kafka.Error:
				return e
			case *kafka.Message:
				k := PartitionKey{partition: e.TopicPartition.Partition, topic: *e.TopicPartition.Topic}
				l, ok := queue.partitions[k]
				if !ok {
					l = []*kafka.Message{}
				}
				l = append(l, e)
			}
		}
	}
}

func commitCoordinator(ctx context.Context, consumer *kafka.Consumer, ackCh <-chan Ack) {
	for {
		select {
		case <-ctx.Done():
			log.Println("commit coordinator stopped")
			return

		case ack, ok := <-ackCh:
			if !ok {
				log.Println("ack channel closed")
				return
			}

			topic := ack.Topic
			offsets := []kafka.TopicPartition{
				{
					Topic:     &topic,
					Partition: ack.Partition,
					Offset:    ack.Offset + 1, // commit next offset
				},
			}

			_, err := consumer.CommitOffsets(offsets)
			if err != nil {
				logger.Log.Error(
					fmt.Sprintf("commit error topic=%s partition=%d next_offset=%d err=%v\n",
						ack.Topic,
						ack.Partition,
						ack.Offset+1,
						err),
				)
				continue
			}

			logger.Log.Error(
				fmt.Sprintf("committed topic=%s partition=%d next_offset=%d\n",
					ack.Topic,
					ack.Partition,
					ack.Offset+1),
			)
		}
	}
}
