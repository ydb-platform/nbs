package fixenv

import "sync"

type scopeInfo struct {
	t T

	m         sync.Mutex
	cacheKeys []cacheKey
}

func newScopeInfo(t T) *scopeInfo {
	return &scopeInfo{
		t: t,
	}
}

func (s *scopeInfo) AddKey(key cacheKey) {
	s.m.Lock()
	defer s.m.Unlock()

	s.cacheKeys = append(s.cacheKeys, key)
}

func (s *scopeInfo) Keys() []cacheKey {
	s.m.Lock()
	defer s.m.Unlock()

	res := make([]cacheKey, len(s.cacheKeys))
	copy(res, s.cacheKeys)
	return res
}
