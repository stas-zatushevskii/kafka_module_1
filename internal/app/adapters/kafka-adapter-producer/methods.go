package kafka_adapter_producer

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Produce synchronous message send to kafka.
func (p *KafkaProducer) Produce(ctx context.Context, message *kafka.Message) error {
	deliveryChan := make(chan kafka.Event, 1)

	err := p.producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}

	select {
	case ev := <-deliveryChan:
		m := ev.(*kafka.Message)
		return m.TopicPartition.Error
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush repeatedly calls producer.Flush until all queued messages are delivered
// or the context timeout is reached. It returns an error when the timeout expires
// and some messages didn't manage to be delivered on time
func (p *KafkaProducer) Flush(c context.Context, timeoutSec int) error {
	ctx, cancel := context.WithTimeout(c, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	for {
		remaining := p.producer.Flush(100) // 100ms
		if remaining == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("kafka flush timed out: %d message(s) not delivered", remaining)
		}
	}
}

func (p *KafkaProducer) Close() {
	p.producer.Close()
}
