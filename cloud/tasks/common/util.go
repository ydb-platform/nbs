package common

import (
	"fmt"
	"math/rand"
	"time"
)

////////////////////////////////////////////////////////////////////////////////

func Assertf(condition bool, message string, args ...any) {
	if !condition {
		panic(fmt.Sprintf(message, args...))
	}
}

////////////////////////////////////////////////////////////////////////////////

func Find(slice []string, value string) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}

	return false
}

func RandomDuration(min time.Duration, max time.Duration) time.Duration {
	rand.Seed(time.Now().UnixNano())
	x := min.Microseconds()
	y := max.Microseconds()

	if y <= x {
		return min
	}

	return time.Duration(x+rand.Int63n(y-x)) * time.Microsecond
}
