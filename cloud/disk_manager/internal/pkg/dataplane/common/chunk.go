package common

import (
	"bytes"
	"hash/crc32"
)

////////////////////////////////////////////////////////////////////////////////

var zeroChunk = make([]byte, 1024*1024)

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

func (chunk Chunk) IsZero() bool {
	for i := 0; i < len(chunk.Data); i += len(zeroChunk) {
		endOffset := i + len(zeroChunk)
		if endOffset > len(chunk.Data) {
			endOffset = len(chunk.Data)
		}
		chunkPart := chunk.Data[i:endOffset]
		if !bytes.Equal(chunkPart, zeroChunk[:len(chunkPart)]) {
			return false
		}
	}
	return true
}
