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
	fmt.Println("Starting Peril client...")
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

	err = pubsub.PublishJSON(chann, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
	if err != nil {
		log.Fatalf("could not PublishJSON: %v\n", err)
	}

	// fmt.Println("Peril game server connected to RabbitMQ")

	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	name, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not read name: %v\n", err)
	}

	fmt.Println("Peril client connection closed.")
}
