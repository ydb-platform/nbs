package common

import (
	"hash/crc32"
)

////////////////////////////////////////////////////////////////////////////////

type Chunk struct {
	ID          string
	Index       uint32
	Data        []byte
	Zero        bool
	StoredInS3  bool
	Compression string
}

func (chunk Chunk) Checksum() uint32 {
	return crc32.ChecksumIEEE(chunk.Data)
}
