package rabbitmq

import (
	"encoding/json"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	channel  *amqp.Channel
	Name     string
	exchange string
}

func New(s string) (*RabbitMQ, error) {
	conn, err := amqp.Dial(s)
	if err != nil {
		return nil, errors.Wrap(err, "connect failed")
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "create channel failed")
	}

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to declare queue")
	}

	mq := new(RabbitMQ)
	mq.channel = ch
	mq.Name = q.Name

	return mq, nil
}

func (mq *RabbitMQ) Bind(exchange string) error {
	err := mq.channel.QueueBind(
		mq.Name,  // queue name
		"",       // routing key
		exchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return errors.Wrap(err, "failed to bind")
	}

	mq.exchange = exchange

	return nil
}

func (mq *RabbitMQ) Send(queue string, body interface{}) error {
	bts, err := json.Marshal(body)
	if err != nil {
		return errors.Wrap(err, "failed to marshal")
	}

	err = mq.channel.Publish(
		"",    // exchange
		queue, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ReplyTo: mq.Name,
			Body:    bts, //
		},
	)
	if err != nil {
		return errors.Wrap(err, "failed to publish")
	}

	return nil
}

func (mq *RabbitMQ) Publish(exchange string, body interface{}) error {
	bts, err := json.Marshal(body)
	if err != nil {
		return errors.Wrap(err, "Failed to marshal")
	}

	err = mq.channel.Publish(
		exchange, // exchange
		"",       // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ReplyTo: mq.Name,
			Body:    bts, //
		},
	)
	if err != nil {
		return errors.Wrap(err, "failed to publish")
	}

	return nil
}

func (mq *RabbitMQ) Consume() (<-chan amqp.Delivery, error) {
	ch, err := mq.channel.Consume(
		mq.Name, // queue
		"",      // consumer
		true,    // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	if err != nil {
		return ch, errors.Wrap(err, "failed to consume")
	}

	return ch, nil
}

func (mq *RabbitMQ) Close() error {
	return mq.channel.Close()
}
