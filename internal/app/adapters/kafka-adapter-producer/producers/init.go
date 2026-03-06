package producers

import (
	"context"
	producer "kafka_module_1/internal/app/adapters/kafka-adapter-producer"
	"kafka_module_1/internal/app/config"
	"time"

	"golang.org/x/sync/errgroup"
)

type MyProducer struct {
	producer *producer.KafkaProducer
	topic    string
}

func New(topic string, kafkaProducer *producer.KafkaProducer) *MyProducer {
	return &MyProducer{
		producer: kafkaProducer,
		topic:    topic,
	}
}

func (p *MyProducer) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		<-ctx.Done()
		if err := p.producer.Flush(ctx, 5); err != nil {
			return err
		}
		p.producer.Close()
		return nil
	})

	ticker := time.NewTicker(config.App.GetTimeout())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.SendOrderData(ctx); err != nil {
				return err
			}
		}
	}
}
