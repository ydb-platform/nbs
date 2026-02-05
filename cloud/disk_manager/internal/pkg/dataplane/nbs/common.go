package nbs

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
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

////////////////////////////////////////////////////////////////////////////////

func resetEncryptionIfNeeded(encryptionDesc *types.EncryptionDesc) {
	// Disks created with the encryption at rest option, or within a folder with
	// encryption at rest enabled, must be mounted without the encryption option.
	// NBS processes encryption on its side.
	rootKmsMode := types.EncryptionMode_ENCRYPTION_WITH_ROOT_KMS_PROVIDED_KEY
	if encryptionDesc != nil {
		if encryptionDesc.Mode == rootKmsMode {
			encryptionDesc.Mode = types.EncryptionMode_NO_ENCRYPTION
		}
	}
}
