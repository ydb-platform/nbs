package nbs

import (
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func min(x, y uint64) uint64 {
	if x > y {
		return y
	}

	return x
}

func validate(chunkSize uint32, blockSize uint32) error {
	if chunkSize == 0 {
		return errors.NewNonRetriableErrorf("chunkSize should not be zero")
	}

	if blockSize == 0 {
		return errors.NewNonRetriableErrorf("blockSize should not be zero")
	}

	if chunkSize%blockSize != 0 {
		return errors.NewNonRetriableErrorf(
			"chunkSize should be multiple of blockSize, chunkSize=%v, blockSize=%v",
			chunkSize,
			blockSize,
		)
	}

	return nil
}
