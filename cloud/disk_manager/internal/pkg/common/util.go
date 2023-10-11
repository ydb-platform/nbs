package common

import (
	"math/rand"
	"time"
)

////////////////////////////////////////////////////////////////////////////////

func Find(slice []string, value string) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}

	return false
}

func RandomShuffle(slice []string) {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(
		len(slice),
		func(i, j int) { slice[i], slice[j] = slice[j], slice[i] },
	)
}

func RandomDuration(
	min time.Duration,
	max time.Duration,
) time.Duration {

	rand.Seed(time.Now().UnixNano())
	x := min.Microseconds()
	y := max.Microseconds()

	if y <= x {
		return min
	}

	return time.Duration(x+rand.Int63n(y-x)) * time.Microsecond
}

func RandomElement(slice []string) string {
	rand.Seed(time.Now().UnixNano())
	return slice[rand.Intn(len(slice))]
}
