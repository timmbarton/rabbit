package rabbit

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/timmbarton/layout/log"
	"github.com/timmbarton/utils/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"
)

type NotificationHandler func(context.Context, amqp.Delivery)

func NewHandler[T any](
	handler func(context.Context, *T) error,
) NotificationHandler {
	return func(ctx context.Context, msg amqp.Delivery) {
		ctx, span := tracing.NewSpan(ctx)
		defer span.End()

		span.SetAttributes(attribute.String("messageId", msg.MessageId))
		log.Debug(
			ctx,
			"received amqp message",
			log.Json("msg", msg),
		)

		n := new(T)

		err := json.Unmarshal(msg.Body, n)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "error on unmarshalling amqp message")
			log.Error(
				ctx,
				"error on unmarshalling amqp message",
				zap.Any("notification.type", fmt.Sprintf("%T", n)),
				log.Json("notification", n),
				zap.Error(err),
			)

			return
		}

		err = handler(ctx, n)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "error on handling notification")
			log.Error(
				ctx,
				"error on handling notification",
				log.Json("msg", msg),
				log.Json("notification", n),
				zap.Error(err),
			)

			err = msg.Reject(true)
			if err != nil {
				span.SetStatus(codes.Error, "error on reject notification: "+err.Error())
				log.Error(
					ctx,
					"error on reject notification",
					zap.Error(err),
				)

				return
			}

			return
		}

		err = msg.Ack(false)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "error on ack amqp message")
			log.Error(
				ctx,
				"error on ack amqp message",
				zap.Error(err),
			)

			return
		}

		return
	}
}

type subscriber struct {
	queueName string
	handler   NotificationHandler
}
