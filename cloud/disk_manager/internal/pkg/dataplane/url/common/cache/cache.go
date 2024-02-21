package cache

import (
	"context"
	"sort"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////

type chunk struct {
	start uint64
	data  []byte
}

func (c *chunk) read(start uint64, data []byte) uint64 {
	startOffset := start - c.start
	endOffset := startOffset + uint64(len(data))
	if endOffset > uint64(len(c.data)) {
		endOffset = uint64(len(c.data))
	}

	copy(data, c.data[startOffset:endOffset])
	return endOffset - startOffset
}

////////////////////////////////////////////////////////////////////////////////

type readFunc = func(context.Context, uint64, []byte) error

type Cache struct {
	readOnCacheMiss readFunc

	chunkPool    sync.Pool
	chunks       map[uint64]*chunk
	chunksMutex  sync.Mutex
	maxCacheSize uint64
	chunkSize    uint64
}

func NewCache(readOnCacheMiss readFunc) *Cache {
	// 4 MiB.
	chunkSize := 4 * 1024 * 1024
	return &Cache{
		readOnCacheMiss: readOnCacheMiss,
		chunkPool: sync.Pool{
			New: func() interface{} {
				return &chunk{
					data: make([]byte, chunkSize),
				}
			},
		},
		chunks: make(map[uint64]*chunk),
		// 1 GiB.
		maxCacheSize: 1024 * 1024 * 1024,
		chunkSize:    uint64(chunkSize),
	}
}

////////////////////////////////////////////////////////////////////////////////

func (c *Cache) readChunk(
	start,
	chunkStart uint64,
	data []byte,
) (uint64, bool) {

	c.chunksMutex.Lock()
	defer c.chunksMutex.Unlock()

	retrievedChunk, ok := c.chunks[chunkStart]
	if !ok {
		return 0, false
	}
	return retrievedChunk.read(start, data), true
}

func (c *Cache) Read(
	ctx context.Context,
	start uint64,
	data []byte,
) error {

	end := start + uint64(len(data))
	for start < end {
		chunkStart := (start / c.chunkSize) * c.chunkSize
		bytesRead, ok := c.readChunk(start, chunkStart, data)

		if !ok {
			// We read at most one chunk, so we will retrieve at most 2 chunks and
			// save them to cache (in case the read data crosses the border between
			// two chunks).
			retrievedChunk := c.chunkPool.Get().(*chunk)
			retrievedChunk.start = chunkStart
			err := c.readOnCacheMiss(ctx, retrievedChunk.start, retrievedChunk.data)
			if err != nil {
				return err
			}

			bytesRead = retrievedChunk.read(start, data)
			c.put(retrievedChunk)
		}

		data = data[bytesRead:]
		start = chunkStart + c.chunkSize
	}

	return nil
}

// Not thread-safe.
func (c *Cache) size() uint64 {
	return uint64(len(c.chunks)) * c.chunkSize
}

func (c *Cache) put(chunk *chunk) {
	c.chunksMutex.Lock()
	defer c.chunksMutex.Unlock()

	if c.size() >= c.maxCacheSize {
		var keys []uint64
		for key := range c.chunks {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

		for _, key := range keys {
			c.chunkPool.Put(c.chunks[key])
			delete(c.chunks, key)

			if c.size() < c.maxCacheSize {
				break
			}
		}
	}

	c.chunks[chunk.start] = chunk
}
