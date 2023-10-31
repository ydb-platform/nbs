package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

func goCacheRead(cache *Cache, start uint64, wg *errgroup.Group) {
	wg.Go(
		func() error {
			data := make([]byte, cache.chunkSize)

			for i := 0; i < 10000; i++ {
				err := cache.Read(
					start,
					data,
					func(start uint64, data []byte) error {
						return nil
					},
				)
				if err != nil {
					return err
				}
			}

			return nil
		},
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestCacheReadConcurrently(t *testing.T) {
	cache := NewCache()
	cache.maxCacheSize = cache.chunkSize
	wg := errgroup.Group{}
	for i := uint64(0); i <= 3; i++ {
		start := (i % 3) * cache.chunkSize
		goCacheRead(cache, start, &wg)
	}
	require.NoError(t, wg.Wait())
}
