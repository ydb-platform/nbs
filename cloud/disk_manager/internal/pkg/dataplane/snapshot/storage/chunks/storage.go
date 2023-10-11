package chunks

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
)

////////////////////////////////////////////////////////////////////////////////

// Interface for communicating with different chunk storages.
type Storage interface {
	ReadChunk(ctx context.Context, chunk *common.Chunk) (err error)

	WriteChunk(
		ctx context.Context,
		referer string,
		chunk common.Chunk,
	) (err error)

	RefChunk(ctx context.Context, referer string, chunkID string) (err error)

	UnrefChunk(ctx context.Context, referer string, chunkID string) (err error)
}
