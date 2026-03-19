package pubsub

import (
	"encoding/json"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange string,
	queueName string,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	chann, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return errors.Join(fmt.Errorf("could not declare and bind channel and queue"), err)
	}

	delivery, err := chann.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume messages: %v", err)
	}

	go func() {
		defer chann.Close()

		for msg := range delivery {
			var data T
			err = json.Unmarshal(msg.Body, &data)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}
			switch handler(data) {
			case Ack:
				msg.Ack(false)
				fmt.Println("Ack")
			case NackDiscard:
				msg.Nack(false, false)
				fmt.Println("NackDiscard")
			case NackRequeue:
				msg.Nack(false, true)
				fmt.Println("NackRequest")
			}
		}
	}()

	return nil
}
