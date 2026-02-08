package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// EventPublisher publishes events to RabbitMQ
type EventPublisher struct {
	cfg    Config
	conn   *amqp.Connection
	ch     *amqp.Channel
	mu     sync.Mutex
	closed bool
}

// NewEventPublisher creates a new event publisher
func NewEventPublisher(cfg Config) (*EventPublisher, error) {
	conn, err := amqp.Dial(cfg.RabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("amqp channel: %w", err)
	}
	// Declare events exchange
	if err := ch.ExchangeDeclare(cfg.EventsExchange, "topic", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("declare exchange %s: %w", cfg.EventsExchange, err)
	}
	return &EventPublisher{cfg: cfg, conn: conn, ch: ch}, nil
}

// PublishEvent publishes an event to the events exchange
func (p *EventPublisher) PublishEvent(ctx context.Context, routingKey string, event interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("publisher closed")
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	err = p.ch.PublishWithContext(ctx, p.cfg.EventsExchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	})
	if err != nil {
		return fmt.Errorf("publish event: %w", err)
	}
	slog.Debug("published event", "routing_key", routingKey)
	return nil
}

// Close closes the connection and channel
func (p *EventPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	_ = p.ch.Close()
	return p.conn.Close()
}

