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
	fmt.Println("Peril game client connected to RabbitMQ!")

	name, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not read name: %v\n", err)
	}

	_, queue, err := pubsub.DeclareAndBind(con, routing.ExchangePerilDirect, routing.PauseKey+"."+name, routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Fatalf("could not declare and bind channel and queue: %v\n", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Peril client connection closed.\n")
}
