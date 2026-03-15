package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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

	chann, err := con.Channel()
	if err != nil {
		log.Fatalf("could not create channel for RabbitMQ: %v\n", err)
	}

	for {
		input := gamelogic.GetInput()
		com := input[0]

		if com == "pause" {
			log.Println("Pausing the game.")
			err = pubsub.PublishJSON(chann, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatalf("could not PublishJSON: %v\n", err)
			}
		} else if com == "resume" {
			log.Println("Resuming the game.")
			err = pubsub.PublishJSON(chann, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatalf("could not PublishJSON: %v\n", err)
			}

		} else if com == "quit" {
			log.Println("Exiting the game.")
			break
		} else {
			log.Printf("Unkown command: %v\n", com)
		}
	}

	fmt.Println("Peril game server connected to RabbitMQ")

	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("RabbitMQ connection closed.")
}
