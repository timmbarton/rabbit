package rabbit

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	contentTypeJson = "application/json"
	schemeAMQP      = "amqp"
)

type ConnectorConfig struct {
	Name                 string `validate:"required"`
	ReconnectionAttempts int    `validate:"required,min=1"`
	URI                  struct {
		Host     string `validate:"required"`
		Port     int    `validate:"required,min=0"`
		Username string `validate:"required"`
		Password string `validate:"required"`
		Vhost    string `validate:"required"`

		CertFile   string // client TLS auth - path to certificate (PEM)
		CACertFile string // client TLS auth - path to CA certificate (PEM)
		KeyFile    string // client TLS auth - path to private key (PEM)
		ServerName string // client TLS auth - server name
	} `validate:"required"`

	Exchange         string
	UseDurableQueues bool
}

func (c *ConnectorConfig) getURI() amqp.URI {
	return amqp.URI{
		Scheme:     schemeAMQP,
		Host:       c.URI.Host,
		Port:       c.URI.Port,
		Username:   c.URI.Username,
		Password:   c.URI.Password,
		Vhost:      c.URI.Vhost,
		CertFile:   c.URI.CertFile,
		CACertFile: c.URI.CACertFile,
		KeyFile:    c.URI.KeyFile,
		ServerName: c.URI.ServerName,
	}
}
