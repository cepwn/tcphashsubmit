package queue

import amqp "github.com/rabbitmq/amqp091-go"

func Open(connStr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(connStr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
