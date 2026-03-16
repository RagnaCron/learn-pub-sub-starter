package main

import (
	"fmt"
	"log"

	"github.com/RagnaCron/learn-pub-sub-starter/internal/gamelogic"
	"github.com/RagnaCron/learn-pub-sub-starter/internal/pubsub"
	"github.com/RagnaCron/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	gamelogic.PrintServerHelp()
	const connectionString = "amqp://guest:guest@localhost:5672/"

	con, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v\n", err)
	}
	defer con.Close()

	chann, queue, err := pubsub.DeclareAndBind(con, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.Durable)
	if err != nil {
		log.Fatalf("could not declare and bind channel and queue: %v\n", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "pause":
			log.Println("Pausing the game.")
			err = pubsub.PublishJSON(chann, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatalf("could not PublishJSON: %v\n", err)
			}
		case "resume":
			log.Println("Resuming the game.")
			err = pubsub.PublishJSON(chann, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatalf("could not PublishJSON: %v\n", err)
			}
		case "quit":
			log.Println("Exiting the game.")
			return
		default:
			log.Printf("Unkown command: %v\n", words[0])
		}
	}
}
