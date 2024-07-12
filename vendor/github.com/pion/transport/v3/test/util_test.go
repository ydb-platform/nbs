// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package test

import (
    "testing"
    "time"
)

func TestCheckRoutines(t *testing.T) {
    // Limit runtime in case of deadlocks
    lim := TimeOut(time.Second * 20)
    defer lim.Stop()

    // Check for leaking routines
    report := CheckRoutines(t)
    defer report()

    go func() {
        time.Sleep(1 * time.Second)
    }()
}
