package rabbit

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/timmbarton/utils/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type NotificationHandler func(context.Context, amqp.Delivery)

func NewHandler[T any](
	handler func(context.Context, *T) error,
) NotificationHandler {
	return func(ctx context.Context, msg amqp.Delivery) {
		ctx, span := tracing.NewSpan(ctx)
		defer span.End()

		span.SetAttributes(attribute.String("messageId", msg.MessageId))
		log.Printf(
			"received amqp message: \ntraceId: %s\n message: %s\n",
			tracing.GetTraceID(span),
			string(msg.Body),
		)

		n := new(T)

		err := json.Unmarshal(msg.Body, n)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "error on unmarshalling amqp message")
			log.Printf("error on unmarshalling amqp message into %T: %s\n", n, err.Error())

			return
		}

		err = handler(ctx, n)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "error on handling notification")
			log.Printf(
				"error on handling notification\nnotification: %s (%T), error: %s\n",
				string(msg.Body),
				n,
				err.Error(),
			)

			err = msg.Reject(true)
			if err != nil {
				span.SetStatus(codes.Error, "error on reject notification: "+err.Error())
				log.Printf("error on reject notification: %s\n", err.Error())

				return
			}

			return
		}

		err = msg.Ack(false)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "error on ack amqp message")
			log.Printf("error on ack amqp message: %s\n", err.Error())

			return
		}

		return
	}
}

type subscriber struct {
	queueName string
	handler   NotificationHandler
}
