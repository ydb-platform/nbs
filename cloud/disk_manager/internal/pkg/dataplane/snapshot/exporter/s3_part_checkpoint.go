package exporter

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// S3ExportCheckpoint stores successful PUTs together with the export state.
// Durable implementations commit both before returning. Parts are ordered
// by number; after is exclusive. An empty page ends the saved parts.
type S3ExportCheckpoint interface {
	ReadParts(
		ctx context.Context,
		after uint32,
		limit uint64,
	) ([]S3ManifestPart, error)
	Save(
		ctx context.Context,
		state *S3ExportState,
		parts []S3ManifestPart,
	) error
}

// Synchronous callers keep confirmations in memory. The legacy callback may
// additionally persist the complete checkpoint, including a CLI progress file.
type memoryS3ExportCheckpoint struct {
	mu      sync.Mutex
	state   *S3ExportState
	save    func(context.Context) error
	ordered []S3ManifestPart
}

func (c *memoryS3ExportCheckpoint) ReadParts(
	ctx context.Context,
	after uint32,
	limit uint64,
) ([]S3ManifestPart, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.ordered == nil {
		c.ordered = append([]S3ManifestPart{}, c.state.Manifest.Parts...)
		sort.Slice(c.ordered, func(i, j int) bool {
			return c.ordered[i].Number < c.ordered[j].Number
		})
	}
	parts := c.ordered
	start := sort.Search(len(parts), func(i int) bool {
		return parts[i].Number > after
	})
	end := start + int(limit)
	if end > len(parts) {
		end = len(parts)
	}
	return append([]S3ManifestPart(nil), parts[start:end]...), nil
}

func (c *memoryS3ExportCheckpoint) Save(
	ctx context.Context,
	state *S3ExportState,
	parts []S3ManifestPart,
) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	state.Manifest.Parts = append(state.Manifest.Parts, parts...)
	if len(parts) != 0 {
		c.ordered = nil
	}
	return c.save(ctx)
}

type s3PartIterator struct {
	checkpoint S3ExportCheckpoint
	pageSize   int
	page       []S3ManifestPart
	after      uint32
	done       bool
}

func newS3PartIterator(
	checkpoint S3ExportCheckpoint,
	options S3ExportOptions,
) *s3PartIterator {

	return &s3PartIterator{
		checkpoint: checkpoint,
		pageSize:   options.PartsReadPageSize,
	}
}

func (i *s3PartIterator) next(ctx context.Context) (*S3ManifestPart, error) {
	if len(i.page) == 0 && !i.done {
		page, err := i.checkpoint.ReadParts(ctx, i.after, uint64(i.pageSize))
		if err != nil {
			return nil, err
		}
		if len(page) > i.pageSize {
			return nil, fmt.Errorf("export checkpoint page exceeds limit")
		}
		previous := i.after
		for _, part := range page {
			if part.Number <= previous {
				return nil, fmt.Errorf(
					"export checkpoint parts are not ordered",
				)
			}
			previous = part.Number
		}
		i.page = page
		i.done = len(page) == 0
	}
	if len(i.page) == 0 {
		return nil, nil
	}
	part := i.page[0]
	i.page = i.page[1:]
	i.after = part.Number
	return &part, nil
}
