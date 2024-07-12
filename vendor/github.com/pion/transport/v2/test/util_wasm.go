// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package test

import (
    "strings"
)

func filterRoutineWASM(stack string) bool {
    return strings.Contains(stack, "runtime.goexit()") // Nested t.Run on Go 1.14 WASM has this routine
}
