//go:build go1.18
// +build go1.18

package fixenv

import "fmt"

// CacheResult is call f once per cache scope (default per test) and cache result (success or error).
// All other calls of the f will return same result.
func CacheResult[TRes any](env Env, f GenericFixtureFunction[TRes], options ...CacheOptions) TRes {
	var cacheOptions CacheOptions
	switch len(options) {
	case 0:
		cacheOptions = CacheOptions{}
	case 1:
		cacheOptions = options[0]
	default:
		panic(fmt.Errorf("max len of cache result cacheOptions is 1, given: %v", len(options)))
	}

	addSkipLevelCache(&cacheOptions)
	var oldStyleFunc FixtureFunction = func() (*Result, error) {
		res, err := f()

		var oldStyleRes *Result
		if res != nil {
			oldStyleRes = &Result{
				Value:            res.Value,
				ResultAdditional: res.ResultAdditional,
			}
		}
		return oldStyleRes, err
	}
	res := env.CacheResult(oldStyleFunc, cacheOptions)
	return res.(TRes)
}

// GenericFixtureFunction - callback function with structured result
type GenericFixtureFunction[ResT any] func() (*GenericResult[ResT], error)

// GenericResult of fixture callback
type GenericResult[ResT any] struct {
	Value ResT
	ResultAdditional
}

// NewGenericResult return result struct and nil error.
// Use it for smaller boilerplate for define generic specifications
func NewGenericResult[ResT any](res ResT) *GenericResult[ResT] {
	return &GenericResult[ResT]{Value: res}
}

func NewGenericResultWithCleanup[ResT any](res ResT, cleanup FixtureCleanupFunc) *GenericResult[ResT] {
	return &GenericResult[ResT]{Value: res, ResultAdditional: ResultAdditional{Cleanup: cleanup}}
}

func addSkipLevelCache(optspp *CacheOptions) {
	(*optspp).additionlSkipExternalCalls++
}
