package fixenv

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

const packageScopeName = "TestMain"

var (
	globalCache               *cache
	globalEmptyFixtureOptions *FixtureOptions

	globalMutex     sync.Mutex
	globalScopeInfo map[string]*scopeInfo
)

func initGlobalState() {
	globalCache = newCache()
	globalEmptyFixtureOptions = &FixtureOptions{}

	globalMutex = sync.Mutex{}
	globalScopeInfo = make(map[string]*scopeInfo)
}

func init() {
	initGlobalState()
}

// EnvT - fixture cache and cleanup engine
// it created from test and pass to fixture function
// manage cache of fixtures, depends from fixture, param, test, scope.
// and call cleanup, when scope closed.
// It can be base to own, more powerful local environments.
type EnvT struct {
	t T
	c *cache

	m      sync.Locker
	scopes map[string]*scopeInfo
}

// New create EnvT from test
func New(t T) *EnvT {
	env := newEnv(t, globalCache, &globalMutex, globalScopeInfo)
	env.onCreate()
	return env
}

// NewEnv create EnvT from test
// Deprecated: use New instead
func NewEnv(t T) *EnvT {
	return New(t)
}

func newEnv(t T, c *cache, m sync.Locker, scopes map[string]*scopeInfo) *EnvT {
	return &EnvT{
		t:      t,
		c:      c,
		m:      m,
		scopes: scopes,
	}
}

// T return test from EnvT created
func (e *EnvT) T() T {
	return e.t
}

// Cache call from fixture and manage call f and cache it.
// Cache must be called direct from fixture - it use runtime stacktrace for
// detect called method - it is part of cache key.
// params - part of cache key. Usually - parameters, passed to fixture.
//
//	it allow use parametrized fixtures with different results.
//	params must be json serializable.
//
// opt - fixture options, nil for default options.
// f - callback - fixture body.
// Cache guarantee for call f exactly once for same Cache called and params combination.
// Deprecated: will be removed in next versions.
// Use EnvT.CacheResult instead
func (e *EnvT) Cache(cacheKey interface{}, opt *FixtureOptions, f FixtureCallbackFunc) interface{} {
	return e.cache(cacheKey, opt, f)
}

// CacheWithCleanup call from fixture and manage call f and cache it.
// CacheWithCleanup must be called direct from fixture - it use runtime stacktrace for
// detect called method - it is part of cache key.
// params - part of cache key. Usually - parameters, passed to fixture.
//
//	it allow use parametrized fixtures with different results.
//	params must be json serializable.
//
// opt - fixture options, nil for default options.
// f - callback - fixture body.
// cleanup, returned from f called while fixture cleanup
// Cache guarantee for call f exactly once for same Cache called and params combination.
// Deprecated: will be removed in next versions.
// Use EnvT.CacheResult instead
func (e *EnvT) CacheWithCleanup(cacheKey interface{}, opt *FixtureOptions, f FixtureCallbackWithCleanupFunc) interface{} {
	if opt == nil {
		opt = &FixtureOptions{}
	}

	var resCleanupFunc FixtureCleanupFunc

	var fWithoutCleanup FixtureCallbackFunc = func() (res interface{}, err error) {
		res, resCleanupFunc, err = f()
		return res, err
	}
	opt.cleanupFunc = func() {
		if resCleanupFunc != nil {
			resCleanupFunc()
		}
	}

	return e.cache(cacheKey, opt, fWithoutCleanup)
}

// CacheResult call f callback once and cache result (ok and error),
// then return same result for all calls of the callback without additional calls
// f with same options calls max once per test (or defined test scope)
func (e *EnvT) CacheResult(f FixtureFunction, options ...CacheOptions) interface{} {
	var cacheOptions CacheOptions
	switch len(options) {
	case 0:
		cacheOptions = CacheOptions{}
	case 1:
		cacheOptions = options[0]
	default:
		panic(fmt.Errorf("max len of cache result cacheOptions is 1, given: %v", len(options)))
	}

	var resCleanupFunc FixtureCleanupFunc

	var fWithoutCleanup FixtureCallbackFunc = func() (res interface{}, err error) {
		result, err := f()
		if result == nil {
			return nil, err
		}

		resCleanupFunc = result.Cleanup
		return result.Value, err
	}

	opt := &FixtureOptions{}
	opt.Scope = cacheOptions.Scope
	opt.additionlSkipExternalCalls = cacheOptions.additionlSkipExternalCalls
	opt.cleanupFunc = resCleanupFunc

	opt.cleanupFunc = func() {
		if resCleanupFunc != nil {
			resCleanupFunc()
		}
	}

	return e.cache(cacheOptions.CacheKey, opt, fWithoutCleanup)

}

// cache must be call from first-level public function
// UserFunction->EnvFunction->cache for good determine caller name
func (e *EnvT) cache(cacheKey interface{}, opt *FixtureOptions, f FixtureCallbackFunc) interface{} {
	if opt == nil {
		opt = globalEmptyFixtureOptions
	}
	key, err := makeCacheKey(e.t.Name(), cacheKey, opt, false)
	if err != nil {
		e.t.Fatalf("failed to create cache key: %v", err)
		// return not reacheble after Fatalf
		return nil
	}

	wrappedF := e.fixtureCallWrapper(key, f, opt)
	res, err := e.c.GetOrSet(key, wrappedF)
	if err != nil {
		if errors.Is(err, ErrSkipTest) {
			e.T().SkipNow()
		} else {
			// Get fixture name
			externalCallerLevel := 4
			var pc = make([]uintptr, externalCallerLevel)
			var extCallerFrame runtime.Frame
			if externalCallerLevel == runtime.Callers(opt.additionlSkipExternalCalls, pc) {
				frames := runtime.CallersFrames(pc)
				frames.Next()                     // callers
				frames.Next()                     // the function
				frames.Next()                     // caller of the function (env private function)
				extCallerFrame, _ = frames.Next() // external caller
			}

			fixtureDesctiption := fmt.Sprintf(
				"%v (%v:%v)",
				extCallerFrame.Function,
				extCallerFrame.File,
				extCallerFrame.Line,
			)
			e.t.Fatalf("failed to call fixture func \"%v\": %v", fixtureDesctiption, err)
		}

		// panic must be not reachable after SkipNow or Fatalf
		panic("fixenv: must be unreachable code after err check in fixture cache")
	}

	return res
}

// tearDown called from base test cleanup
// it clean env cache and call fixture's cleanups for the scope.
func (e *EnvT) tearDown() {
	e.m.Lock()
	defer e.m.Unlock()

	testName := e.t.Name()
	if si, ok := e.scopes[testName]; ok {
		cacheKeys := si.Keys()
		e.c.DeleteKeys(cacheKeys...)
		delete(e.scopes, testName)
	} else {
		e.t.Fatalf("unexpected call env tearDown for test: %q", testName)
	}
}

// onCreate register env in internal stuctures.
func (e *EnvT) onCreate() {
	e.m.Lock()
	defer e.m.Unlock()

	testName := e.t.Name()
	if _, ok := e.scopes[testName]; ok {
		e.t.Fatalf("Env exist already for scope: %q", testName)
	} else {
		e.scopes[testName] = newScopeInfo(e.t)
		e.t.Cleanup(e.tearDown)
	}
}

// makeCacheKey generate cache key
// must be called from first level of env functions - for detect external caller
func makeCacheKey(testname string, params interface{}, opt *FixtureOptions, testCall bool) (cacheKey, error) {
	externalCallerLevel := 5
	var pc = make([]uintptr, externalCallerLevel)
	var extCallerFrame runtime.Frame
	if externalCallerLevel == runtime.Callers(opt.additionlSkipExternalCalls, pc) {
		frames := runtime.CallersFrames(pc)
		frames.Next()                     // callers
		frames.Next()                     // the function
		frames.Next()                     // caller of the function (env private function)
		frames.Next()                     // caller of private function (env public function)
		extCallerFrame, _ = frames.Next() // external caller
	}
	scopeName := makeScopeName(testname, opt.Scope)
	return makeCacheKeyFromFrame(params, opt.Scope, extCallerFrame, scopeName, testCall)
}

func makeCacheKeyFromFrame(params interface{}, scope CacheScope, f runtime.Frame, scopeName string, testCall bool) (cacheKey, error) {
	switch {
	case f.Function == "":
		return "", errors.New("failed to detect caller func name")
	case f.File == "":
		return "", errors.New("failed to detect caller func file")
	default:
		// pass
	}

	key := struct {
		Scope        CacheScope  `json:"scope"`
		ScopeName    string      `json:"scope_name"`
		FunctionName string      `json:"func"`
		FileName     string      `json:"fname"`
		Params       interface{} `json:"params"`
	}{
		Scope:        scope,
		ScopeName:    scopeName,
		FunctionName: f.Function,
		FileName:     f.File,
		Params:       params,
	}
	if testCall {
		key.FileName = ".../" + filepath.Base(key.FileName)
	}

	keyBytes, err := json.Marshal(key)
	if err != nil {
		return "", fmt.Errorf("failed to serialize params to json: %v", err)
	}
	return cacheKey(keyBytes), nil

}

func (e *EnvT) fixtureCallWrapper(key cacheKey, f FixtureCallbackFunc, opt *FixtureOptions) FixtureCallbackFunc {
	return func() (res interface{}, err error) {
		scopeName := makeScopeName(e.t.Name(), opt.Scope)

		e.m.Lock()
		si := e.scopes[scopeName]
		e.m.Unlock()

		if si == nil {
			e.t.Fatalf("Unexpected scope: %q. Initialize package scope before use."+
				"For scope %s use fixenv.RunTests", scopeName, packageScopeName)
			// not reachable
			return nil, nil
		}

		defer func() {
			si.AddKey(key)
		}()

		res, err = f()

		if opt.cleanupFunc != nil {
			si.t.Cleanup(opt.cleanupFunc)
		}

		return res, err
	}
}

func makeScopeName(testName string, scope CacheScope) string {
	switch scope {
	case ScopePackage:
		return packageScopeName
	case ScopeTest:
		return testName
	case ScopeTestAndSubtests:
		parts := strings.SplitN(testName, "/", 2)
		return parts[0]
	default:
		panic(fmt.Sprintf("Unknown scope: %v", scope))
	}
}
