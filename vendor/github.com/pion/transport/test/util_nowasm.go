//go:build !wasm
// +build !wasm

package test

func filterRoutineWASM(stack string) bool {
    return false
}
