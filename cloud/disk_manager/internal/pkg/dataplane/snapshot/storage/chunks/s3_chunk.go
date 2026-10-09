package chunks

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/compressor"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

// The object body must already have its backup encryption envelope removed.
func DecodeS3Chunk(
	ctx context.Context,
	object persistence.S3Object,
	chunk *common.Chunk,
	metrics metrics.Metrics,
) error {

	metadata, err := newS3Metadata(object.Metadata)
	if err != nil {
		return err
	}

	logging.Debug(
		ctx,
		"read chunk from s3 {id: %q, checksum: %v, compression: %q}",
		chunk.ID,
		metadata.checksum,
		metadata.compression,
	)

	err = compressor.Decompress(
		metadata.compression,
		object.Data,
		chunk.Data,
		metrics,
	)
	if err != nil {
		return err
	}

	actualChecksum := chunk.Checksum()
	if metadata.checksum != actualChecksum {
		return task_errors.NewNonRetriableErrorf(
			"ReadChunk: s3 chunk checksum mismatch: expected %v, actual %v",
			metadata.checksum,
			actualChecksum,
		)
	}

	return nil
}
