package main

import (
	"fmt"

	"github.com/RagnaCron/learn-pub-sub-starter/internal/gamelogic"
)

func handlerMove(gs *gamelogic.GameState) func(move gamelogic.ArmyMove) {
	return func(move gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(move)
	}
}
