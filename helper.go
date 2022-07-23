package scheduler

import (
	"math/rand"
	"time"
)

func randomNumber(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min+1) + 1
}
