package rabbit

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

type Consumer struct {
	cfg         ConnectorConfig
	conn        *Connector
	errGroup    errgroup.Group
	cancel      context.CancelFunc
	subscribers []subscriber
}

func NewConsumer(cfg ConnectorConfig) *Consumer {
	return &Consumer{
		cfg:         cfg,
		conn:        NewConnector(cfg),
		errGroup:    errgroup.Group{},
		cancel:      nil,
		subscribers: make([]subscriber, 0),
	}
}

func (c *Consumer) Start(ctx context.Context) (err error) {
	err = c.conn.Start(ctx)
	if err != nil {
		return err
	}

	subscribersCtx := context.Context(nil)
	subscribersCtx, c.cancel = context.WithCancel(context.Background())

	for _, s := range c.subscribers {
		err = c.runSubscriber(subscribersCtx, s)
		if err != nil {
			return err
		}
	}

	return nil
}
func (c *Consumer) Stop(ctx context.Context) (err error) {
	if c.cancel != nil {
		c.cancel()
	}

	if c.conn != nil {
		err = c.conn.Stop(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}
func (c *Consumer) GetName() string {
	if c.cfg.Name != "" {
		return c.cfg.Name
	}

	return "AMQP Consumer"
}

func (c *Consumer) AddSubscriber(queueName string, handler NotificationHandler) {
	if handler == nil {
		panic("handler cant be nil")
	}

	c.subscribers = append(c.subscribers, subscriber{
		queueName: queueName,
		handler:   handler,
	})
}
func randomizeConsumerName(name string) string {
	const suffixLen = 6

	maxSuffixValue := int64(math.Pow(16, suffixLen))
	format := "%s (%0" + strconv.Itoa(suffixLen) + "x)" // "%s (%06x)"

	return fmt.Sprintf(format, name, rand.N(maxSuffixValue))
}
func (c *Consumer) runSubscriber(ctx context.Context, s subscriber) error {
	c.errGroup.Go(func() error {
		ch := (*amqp.Channel)(nil)
		err := error(nil)

		for {
			err = c.conn.tryGetChannel(ctx, &ch)
			if err != nil {
				return err
			}

			messages := (<-chan amqp.Delivery)(nil)

			for ch != nil && !ch.IsClosed() {
				messages, err = ch.Consume(s.queueName,
					randomizeConsumerName(c.cfg.Name),
					false,
					false,
					false,
					false,
					nil,
				)
				if err != nil {
					break
				}

				channelClosed := false
				for {
					select {
					case msg, ok := <-messages:
						if !ok {
							channelClosed = true
							break
						}

						s.handler(ctx, msg)
					case <-ctx.Done():
						return context.Canceled
					}

					if channelClosed {
						break
					}
				}
			}
		}
	})

	return nil
}
