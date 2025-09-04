package fixenv

import (
	"errors"
	"sync"
)

type cache struct {
	m        sync.RWMutex
	store    map[cacheKey]cacheVal
	setLocks map[cacheKey]*sync.Once
}

type cacheKey string

type cacheVal struct {
	res *Result
	err error
}

func newCache() *cache {
	return &cache{
		store:    make(map[cacheKey]cacheVal),
		setLocks: make(map[cacheKey]*sync.Once),
	}
}

// GetOrSet atomic get exist values from cache or call f for set new value and return it.
// it has guarantee about only one f will execute same time for the key.
// but many f may execute simultaneously for different keys
func (c *cache) GetOrSet(key cacheKey, f FixtureFunction) (*Result, error) {
	res, ok := c.get(key)
	if ok {
		return res.res, res.err
	}

	c.setOnce(key, f)

	res, _ = c.get(key)
	return res.res, res.err
}

func (c *cache) DeleteKeys(keys ...cacheKey) {
	c.m.Lock()
	defer c.m.Unlock()

	for _, key := range keys {
		delete(c.store, key)
		delete(c.setLocks, key)
	}
}

func (c *cache) get(key cacheKey) (cacheVal, bool) {
	c.m.RLock()
	defer c.m.RUnlock()
	val, ok := c.store[key]
	return val, ok
}

func (c *cache) setOnce(key cacheKey, f FixtureFunction) {
	c.m.Lock()
	setOnce := c.setLocks[key]
	if setOnce == nil {
		setOnce = &sync.Once{}
		c.setLocks[key] = setOnce
	}
	c.m.Unlock()

	setOnce.Do(func() {
		var err = errors.New("unexpected exit from function")
		var res *Result

		// save result must be deferred because f() may stop goroutine without result
		// for example by panic or GoExit
		defer func() {
			c.m.Lock()
			c.store[key] = cacheVal{res: res, err: err}
			c.m.Unlock()
		}()

		res, err = f()
	})
}
