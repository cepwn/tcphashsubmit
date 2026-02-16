package queue

import (
	"encoding/json"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SubmissionEvent struct {
	Username  string
	Timestamp time.Time
}

type Delivery struct {
	Body []byte
	Ack  func(multiple bool) error
}

type RabbitMQSubmissionQueue struct {
	conn          *amqp.Connection
	name          string
	ch            *amqp.Channel
	notifyConfirm <-chan amqp.Confirmation
}

func NewRabbitMQSubmissionQueue(amqpConn *amqp.Connection) (*RabbitMQSubmissionQueue, error) {
	ch, err := amqpConn.Channel()
	if err != nil {
		return nil, err
	}
	err = ch.Confirm(false)
	if err != nil {
		return nil, err
	}
	name := "submission_queue"
	_, err = ch.QueueDeclare(
		name,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		return nil, err
	}
	return &RabbitMQSubmissionQueue{
		conn:          amqpConn,
		name:          name,
		ch:            ch,
		notifyConfirm: ch.NotifyPublish(make(chan amqp.Confirmation, 1)),
	}, nil
}

type SubmissionPublisher interface {
	Publish(event SubmissionEvent) error
	Close() error
}

type SubmissionConsumer interface {
	Subscribe() (<-chan Delivery, error)
	Close() error
}

func (q *RabbitMQSubmissionQueue) Publish(event SubmissionEvent) error {
	se, err := json.Marshal(event)
	if err != nil {
		return err
	}
	err = q.ch.Publish("", q.name, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         se,
	})
	if err != nil {
		return err
	}
	confirm := <-q.notifyConfirm
	if !confirm.Ack {
		return errors.New("message failed to publish")
	}
	return nil
}

func (q *RabbitMQSubmissionQueue) Subscribe() (<-chan Delivery, error) {
	if err := q.ch.Qos(
		1,     // prefetchCount
		0,     // prefetchSize
		false, // global
	); err != nil {
		return nil, err
	}

	deliveries, err := q.ch.Consume(
		q.name,
		"",    // Consumer
		false, // Auto-Ack
		false, // Exclusive
		false, // No-local
		false, // No-Wait
		nil,   // Args
	)
	if err != nil {
		return nil, err
	}

	out := make(chan Delivery)
	go func() {
		defer close(out)
		for d := range deliveries {
			out <- Delivery{
				Body: d.Body,
				Ack:  d.Ack,
			}
		}
	}()
	return out, nil
}

func (q *RabbitMQSubmissionQueue) Close() error {
	err := q.ch.Close()
	if err != nil {
		return err
	}
	err = q.conn.Close()
	if err != nil {
		return err
	}

	return nil
}
