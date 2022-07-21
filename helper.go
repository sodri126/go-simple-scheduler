package scheduler

import (
	"math/rand"
	"time"
)

func subtractDateTime(dateTime time.Time) (duration time.Duration, err error) {
	var (
		now         = time.Now().UTC()
		dateTimeUTC = dateTime.UTC()
	)

	if dateTimeUTC.Before(now) {
		err = ErrDateTimeLessThanNow
		return
	}

	duration = dateTimeUTC.Sub(now)
	return
}

func randomNumber(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min+1) + 1
}
