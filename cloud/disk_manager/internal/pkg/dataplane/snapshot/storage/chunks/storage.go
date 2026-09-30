package chunks

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

// Interface for communicating with different chunk storages.
type Storage interface {
	ReadChunk(ctx context.Context, chunk *common.Chunk) (err error)

	ReadChunkBlob(
		ctx context.Context,
		chunkID string,
	) (object persistence.S3Object, err error)

	WriteChunk(
		ctx context.Context,
		referer string,
		chunk common.Chunk,
	) (err error)

	RefChunk(ctx context.Context, referer string, chunkID string) (err error)

	UnrefChunk(
		ctx context.Context,
		referer string,
		chunkID string,
	) (deleted bool, err error)
}
