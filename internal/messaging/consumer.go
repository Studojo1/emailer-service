package messaging

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/studojo/emailer-service/internal/handlers"
)

// Config holds RabbitMQ configuration
type Config struct {
	RabbitMQURL      string
	EventsExchange   string
	EventsQueue      string
	EventsBindKey    string
}

// DefaultConfig returns default messaging configuration
func DefaultConfig(rabbitURL string) Config {
	return Config{
		RabbitMQURL:    rabbitURL,
		EventsExchange: "cp.events",
		EventsQueue:    "emailer.events",
		EventsBindKey:  "event.*",
	}
}

// Consumer consumes email events from RabbitMQ
type Consumer struct {
	cfg     Config
	handler *handlers.EventHandler
}

// NewConsumer creates a new email event consumer
func NewConsumer(cfg Config, handler *handlers.EventHandler) *Consumer {
	return &Consumer{
		cfg:     cfg,
		handler: handler,
	}
}

// Run starts consuming events from RabbitMQ
func (c *Consumer) Run(ctx context.Context) error {
	conn, err := amqp.Dial(c.cfg.RabbitMQURL)
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("amqp channel: %w", err)
	}
	defer ch.Close()

	// Declare exchange
	if err := ch.ExchangeDeclare(c.cfg.EventsExchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	// Declare queue
	queue, err := ch.QueueDeclare(c.cfg.EventsQueue, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	// Bind queue to exchange
	if err := ch.QueueBind(queue.Name, c.cfg.EventsBindKey, c.cfg.EventsExchange, false, nil); err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}

	deliveries, err := ch.Consume(queue.Name, "emailer-service", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	slog.Info("email event consumer started", "queue", queue.Name, "exchange", c.cfg.EventsExchange, "bind_key", c.cfg.EventsBindKey)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("deliveries closed")
			}

			slog.Debug("received email event", "routing_key", d.RoutingKey, "body_size", len(d.Body))

			if err := c.handler.ProcessEvent(ctx, d.RoutingKey, d.Body); err != nil {
				slog.Error("handle event failed", "routing_key", d.RoutingKey, "error", err)
				_ = d.Nack(false, true) // Requeue on error
				continue
			}

			slog.Info("event processed successfully", "routing_key", d.RoutingKey)
			_ = d.Ack(false)
		}
	}
}

// RunWithRetry runs the consumer with automatic reconnection
func RunWithRetry(ctx context.Context, consumer *Consumer, backoff time.Duration) {
	slog.Info("starting email event consumer with retry")
	for {
		err := consumer.Run(ctx)
		if ctx.Err() != nil {
			slog.Info("email event consumer context cancelled")
			return
		}
		slog.Warn("email event consumer stopped", "error", err, "retrying in", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			slog.Info("retrying email event consumer")
		}
	}
}

