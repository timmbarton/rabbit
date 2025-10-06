package rabbit

import (
	"context"
	"errors"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/timmbarton/layout/log"
	"github.com/timmbarton/utils/tracing"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Connector struct {
	cfg      ConnectorConfig
	uri      amqp.URI
	conn     *amqp.Connection
	errGroup errgroup.Group
	m        sync.Mutex
}

func NewConnector(cfg ConnectorConfig) *Connector {
	return &Connector{
		cfg:      cfg,
		uri:      cfg.getURI(),
		conn:     nil,
		errGroup: errgroup.Group{},
	}
}

func (c *Connector) Start(ctx context.Context) (err error) {
	err = c.tryConnect(ctx)
	if err != nil {
		return err
	}

	return nil
}
func (c *Connector) Stop(_ context.Context) (err error) {
	if c.conn == nil || c.conn.IsClosed() {
		return nil
	}

	err = c.conn.Close()
	if err != nil {
		return err
	}

	return nil
}
func (c *Connector) GetName() string {
	if c.cfg.Name != "" {
		return c.cfg.Name
	}

	return "AMQP Connector"
}

func (c *Connector) tryConnect(ctx context.Context) (err error) {
	ctx, span := tracing.NewSpan(ctx)
	defer span.End()

	c.m.Lock()
	defer c.m.Unlock()

	if c.conn != nil && !c.conn.IsClosed() {
		return nil
	}

	errChan := make(chan error, 1)

	go func() {
		connErr := error(nil)

		for range c.cfg.ReconnectionAttempts {
			c.conn, connErr = amqp.Dial(c.uri.String())
			if connErr != nil {
				log.Error(ctx, "error on amqp.Dial", zap.Error(connErr))
				continue
			}

			break
		}

		errChan <- connErr
		close(errChan)
	}()

	ok := false

	select {
	case err, ok = <-errChan:
		if !ok {
			return errors.New("!ok on connect to server")
		}
		if err != nil {
			log.Error(ctx, "cant connect to amqp server", zap.Error(err))
			return err
		}
	case <-ctx.Done():
		return context.Canceled
	}

	c.errGroup.Go(func() error {
		closingErr := <-c.conn.NotifyClose(make(chan *amqp.Error))
		if closingErr != nil {
			log.Error(ctx, "AMQP Consumer closed with error", zap.Error(closingErr))
		} else {
			return nil
		}

		return c.tryConnect(context.Background())
	})

	return nil
}
func (c *Connector) tryGetChannel(ctx context.Context, ch **amqp.Channel) (err error) {
	ctx, span := tracing.NewSpan(ctx)
	defer span.End()

	if ch == nil {
		return errors.New("nil ptr for amqp channel")
	}

	if *ch != nil && !(*ch).IsClosed() {
		return nil
	}

	err = c.tryConnect(ctx)
	if err != nil {
		return err
	}

	errChan := make(chan error, 1)

	go func() {
		connErr := error(nil)

		for range c.cfg.ReconnectionAttempts {
			*ch, connErr = c.conn.Channel()
			if connErr != nil {
				log.Error(ctx, "error on create amqp channel", zap.Error(connErr))
				continue
			}

			break
		}

		errChan <- connErr
		close(errChan)
	}()

	select {
	case err = <-errChan:
		return err
	case <-ctx.Done():
		return context.Canceled
	}
}
