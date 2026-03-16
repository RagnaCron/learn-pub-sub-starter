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

	gameState := gamelogic.NewGameState(name)

	_, queue, err := pubsub.DeclareAndBind(con, routing.ExchangePerilDirect, routing.PauseKey+"."+name, routing.PauseKey, pubsub.Transient)
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
		case "spawn":
			err = gameState.CommandSpawn(words)
			if err != nil {
				log.Fatalf("could not spawn pawn: %v\n", err)
			}
		case "move":
			_, err := gameState.CommandMove(words)
			if err != nil {
				log.Fatalf("could not move pawn: %v\n", err)
			}
			fmt.Println("Moved unit!")
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Printf("Command not found: %v\n", words)
		}
	}
}
