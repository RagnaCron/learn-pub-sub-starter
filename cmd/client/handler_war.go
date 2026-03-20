package main

import (
	"fmt"

	"github.com/RagnaCron/learn-pub-sub-starter/internal/gamelogic"
	"github.com/RagnaCron/learn-pub-sub-starter/internal/pubsub"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		outcome, winner, loser := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			PublishGameLog(fmt.Sprintf("%s won a war against %s", winner, loser))
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			PublishGameLog(fmt.Sprintf("%s won a war against %s", winner, loser))
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			PublishGameLog(fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser))
			return pubsub.Ack
		default:
			fmt.Println("error: unknown war outcome")
			return pubsub.NackDiscard
		}
	}
}
