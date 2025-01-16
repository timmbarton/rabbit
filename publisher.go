package rabbit

import (
	"context"
	"encoding/json"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/timmbarton/utils/tracing"
)

type Publisher struct {
	cfg           ConnectorConfig
	conn          *Connector
	channels      map[string]*channelWithMutex // queueName -> channelWithMutex
	channelsMutex sync.Mutex
}

type channelWithMutex struct {
	ch *amqp.Channel
	m  sync.Mutex
}

func NewPublisher(cfg ConnectorConfig) *Publisher {
	return &Publisher{
		cfg:           cfg,
		conn:          NewConnector(cfg),
		channels:      make(map[string]*channelWithMutex),
		channelsMutex: sync.Mutex{},
	}
}

func (c *Publisher) Start(ctx context.Context) (err error) {
	err = c.conn.Start(ctx)
	if err != nil {
		return err
	}

	return nil
}
func (c *Publisher) Stop(ctx context.Context) (err error) {
	if c.conn != nil {
		err = c.conn.Stop(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}
func (c *Publisher) GetName() string {
	if c.cfg.Name != "" {
		return c.cfg.Name
	}

	return "AMQP Publisher"
}

func (c *Publisher) Publish(ctx context.Context, queueName string, data any) error {
	ctx, span := tracing.NewSpan(ctx)
	defer span.End()

	// берем закешированный канал
	c.channelsMutex.Lock()
	chwm, channelExists := c.channels[queueName]
	if !channelExists {
		chwm = &channelWithMutex{
			ch: nil,
			m:  sync.Mutex{},
		}

		c.channels[queueName] = chwm
	}
	c.channelsMutex.Unlock()

	chwm.m.Lock()
	// проверяем, что канал работает или получаем новый
	err := c.conn.tryGetChannel(ctx, &chwm.ch)
	if err != nil {
		return err
	}

	chwm.m.Unlock()

	// если канала не было, значит и очередь мы ещё не декларировали
	if !channelExists {
		_, err = chwm.ch.QueueDeclare(
			queueName,
			c.cfg.UseDurableQueues,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	// работаем только с json
	dataJson, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// отправляем сообщение
	err = chwm.ch.PublishWithContext(
		ctx,
		c.cfg.Exchange,
		queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: contentTypeJson,
			Body:        dataJson,
		},
	)
	if err != nil {
		return err
	}

	return nil
}
