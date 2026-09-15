package common

import (
	"bytes"
	"hash/crc32"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

var zeroes = make([]byte, 1024*1024)

// Zero selects DefaultChunkSize. Explicit sizes must be multiples of 4 MiB.
func ValidateChunkSize(chunkSize uint32) error {
	if chunkSize%DefaultChunkSize != 0 {
		return errors.NewNonRetriableErrorf(
			"chunk size must be at least 4 MiB and a multiple of 4 MiB, got %v bytes",
			chunkSize,
		)
	}
	return nil
}

func ValidateSnapshotChunkSize(chunkSize uint32, useS3 bool) error {
	err := ValidateChunkSize(chunkSize)
	if err != nil {
		return err
	}
	if !useS3 && chunkSize != 0 && chunkSize != DefaultChunkSize {
		return errors.NewNonRetriableErrorf(
			"non-default snapshot chunk size %v requires S3; YDB snapshots use %v bytes",
			chunkSize,
			DefaultChunkSize,
		)
	}
	return nil
}

type Chunk struct {
	ID           string
	Index        uint32
	Data         []byte
	Zero         bool
	StoredInS3   bool
	Compression  string
	StorageClass string
}

func (chunk Chunk) Checksum() uint32 {
	return crc32.ChecksumIEEE(chunk.Data)
}

func (chunk Chunk) CheckDataIsAllZeroes() bool {
	for i := 0; i < len(chunk.Data); i += len(zeroes) {
		endOffset := i + len(zeroes)

		if endOffset > len(chunk.Data) {
			endOffset = len(chunk.Data)
		}

		dataToCheck := chunk.Data[i:endOffset]

		if !bytes.Equal(dataToCheck, zeroes[:len(dataToCheck)]) {
			return false
		}
	}
	return true
}
