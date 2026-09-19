package nbs

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func min(x, y uint64) uint64 {
	if x > y {
		return y
	}

	return x
}

func validate(blockCount uint64, chunkSize uint32, blockSize uint32) error {
	if chunkSize == 0 {
		return errors.NewNonRetriableErrorf("chunkSize should not be zero")
	}

	if blockSize == 0 {
		return errors.NewNonRetriableErrorf("blockSize should not be zero")
	}
	if blockSize > common.DefaultChunkSize || common.DefaultChunkSize%blockSize != 0 {
		return errors.NewNonRetriableErrorf(
			"blockSize should divide maximum request size %v, blockSize=%v",
			common.DefaultChunkSize,
			blockSize,
		)
	}

	if chunkSize%blockSize != 0 {
		return errors.NewNonRetriableErrorf(
			"chunkSize should be multiple of blockSize, chunkSize=%v, blockSize=%v",
			chunkSize,
			blockSize,
		)
	}

	blocksInChunk := uint64(chunkSize / blockSize)

	if blockCount%blocksInChunk != 0 {
		return errors.NewNonRetriableErrorf(
			"blockCount should be multiple of blocksInChunk, blockCount=%v, blocksInChunk=%v",
			blockCount,
			blocksInChunk,
		)
	}

	return nil
}
