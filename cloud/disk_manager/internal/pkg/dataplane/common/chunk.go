package common

import (
	"bytes"
	"fmt"
	"hash/crc32"
)

////////////////////////////////////////////////////////////////////////////////

// ChunkSize is the size of a dataplane snapshot chunk. Snapshot chunk maps
// and raw export offsets are built from it.
const ChunkSize = 4 * 1024 * 1024

var zeroes = make([]byte, 1024*1024)

type Chunk struct {
	ID           string
	Index        uint32
	Data         []byte
	Zero         bool
	StoredInS3   bool
	Compression  string
	StorageClass string
}

// String keeps chunk formatting cheap: without it fmt reflects over the whole
// multi-megabyte Data slice, e.g. in test mock argument diffs.
func (chunk Chunk) String() string {
	return fmt.Sprintf(
		"{ID: %v, Index: %v, Zero: %v, StoredInS3: %v, Compression: %q, StorageClass: %q, DataSize: %v}",
		chunk.ID,
		chunk.Index,
		chunk.Zero,
		chunk.StoredInS3,
		chunk.Compression,
		chunk.StorageClass,
		len(chunk.Data),
	)
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
