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

	publishCh, err := con.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	name, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not read name: %v\n", err)
	}

	gameState := gamelogic.NewGameState(name)

	err = pubsub.SubscribeJSON(con, routing.ExchangePerilDirect, routing.PauseKey+"."+name, routing.PauseKey, pubsub.Transient, handlerPause(gameState))
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v\n", err)
	}

	err = pubsub.SubscribeJSON(
		con,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+".*."+name,
		routing.ArmyMovesPrefix+".*",
		pubsub.Transient,
		handlerMove(gameState),
	)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "spawn":
			err = gameState.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
			}
		case "move":
			move, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}

			pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+".*."+name, move)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("Moved %v units to %s\n", len(move.Units), move.ToLocation)

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
