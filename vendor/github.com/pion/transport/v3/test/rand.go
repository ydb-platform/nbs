// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package test

import (
    crand "crypto/rand"
    "errors"
    "fmt"
    mrand "math/rand"
)

var errRequestTooLargeBuffer = errors.New("requested too large buffer")

type randomizer struct {
    randomness []byte
}

func initRand() randomizer {
    // read 1MB of randomness
    randomness := make([]byte, 1<<20)
    if _, err := crand.Read(randomness); err != nil {
        fmt.Println("Failed to initiate randomness:", err) // nolint
    }
    return randomizer{
        randomness: randomness,
    }
}

func (r *randomizer) randBuf(size int) ([]byte, error) {
    n := len(r.randomness) - size
    if n < 1 {
        return nil, fmt.Errorf("%w (%d). max is %d", errRequestTooLargeBuffer, size, len(r.randomness))
    }

    start := mrand.Intn(n) //nolint:gosec
    return r.randomness[start : start+size], nil
}
