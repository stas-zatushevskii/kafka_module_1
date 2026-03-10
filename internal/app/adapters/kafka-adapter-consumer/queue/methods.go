package queue

import (
	"context"
	"errors"
	"fmt"
	"kafka_module_1/internal/pkg/logger"
	"time"

	workerPool "kafka_module_1/internal/pkg/worker-pool"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

// ConsumeSingleMode continuously polls and processes Kafka messages using the provided handler;
// stops only on commit failure, otherwise logs errors and keeps running.
func (queue *KafkaQueue) ConsumeSingleMode(ctx context.Context, businessLogicFunc func(context.Context, *kafka.Message) error) error {

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
}

func (queue *KafkaQueue) ConsumeBatchMode(ctx context.Context, businessLogicFunc func(context.Context, []*kafka.Message) error) error {
	return queue.ProcessBatchMessages(ctx, businessLogicFunc)
}

// ProcessOneMessage polls once, invokes the handler for a single Kafka message,
// and commits the offset on success (returns CommitFailed on commit error).
func (queue *KafkaQueue) ProcessOneMessage(businessLogicFunc func(context.Context, *kafka.Message) error) error {
	ctx := context.TODO()

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

func (queue *KafkaQueue) ProcessBatchMessages(ctx context.Context, businessLogicFunc func(context.Context, []*kafka.Message) error) error {
	g, GCtx := errgroup.WithContext(ctx)

	ticker := time.NewTicker(3 * time.Second)        // todo: get value from cfg
	queue.wp = workerPool.NewWorkerPool(GCtx, 4, 20) // todo: get value from cfg
	commitChan := make(chan Ack)

	g.Go(func() error {
		return commitCoordinator(GCtx, queue.kafkaConsumer, commitChan)
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				for k := range queue.partitions {
					batch := append([]*kafka.Message(nil), queue.partitions[k]...)
					queue.partitions[k] = queue.partitions[k][:0]

					key := PartitionKey{
						topic:     k.topic,
						partition: k.partition,
					}

					fn := func(ctx context.Context) error {
						return businessLogicFunc(GCtx, batch)
					}

					task := workerPool.Task{
						Key: key.String(),
						Fn:  fn,
					}

					queue.wp.Set(task)
					commitChan <- Ack{
						Topic:     k.topic,
						Partition: k.partition,
						Offset:    kafka.Offset(len(batch)),
					}
					return nil
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
					l := queue.partitions[k]
					l = append(l, e)
					queue.partitions[k] = l
				}
			}
		}
	})
	return g.Wait()
}

func commitCoordinator(ctx context.Context, consumer *kafka.Consumer, ackCh <-chan Ack) error {
	for {
		select {
		case <-ctx.Done():
			return errors.New("context canceled")

		case ack, ok := <-ackCh:
			if !ok {
				return errors.New("commit coordinator channel closed")
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
				return CommitFailed
			}

			logger.Log.Info(
				fmt.Sprintf("committed topic=%s partition=%d next_offset=%d\n",
					ack.Topic,
					ack.Partition,
					ack.Offset+1),
			)
		}
	}
}

func (k *PartitionKey) String() string {
	return fmt.Sprintf("%s-%d", k.topic, k.partition)
}
