package cache

import (
	"context"
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
				err := cache.Read(context.Background(), start, data)
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
	cache := NewCache(
		func(context.Context, uint64, []byte) error {
			return nil
		},
	)
	cache.maxCacheSize = cache.chunkSize
	wg := errgroup.Group{}
	for i := uint64(0); i <= 3; i++ {
		start := (i % 3) * cache.chunkSize
		goCacheRead(cache, start, &wg)
	}
	require.NoError(t, wg.Wait())
}
