package common

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func TestValidateChunkSize(t *testing.T) {
	for _, chunkSize := range []uint32{0, DefaultChunkSize, 8 * 1024 * 1024, 12 * 1024 * 1024, 20 * 1024 * 1024} {
		require.NoError(t, ValidateChunkSize(chunkSize))
	}
	for _, chunkSize := range []uint32{1, 32 * 1024, 1024 * 1024, 2 * 1024 * 1024, DefaultChunkSize - 1, DefaultChunkSize + 1, 6 * 1024 * 1024} {
		err := ValidateChunkSize(chunkSize)
		require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())
		require.Contains(t, err.Error(), "multiple of 4 MiB")
	}
}

func TestValidateSnapshotChunkSize(t *testing.T) {
	for _, useS3 := range []bool{false, true} {
		for _, chunkSize := range []uint32{0, DefaultChunkSize} {
			require.NoError(t, ValidateSnapshotChunkSize(chunkSize, useS3))
		}
		for _, chunkSize := range []uint32{1, 1024 * 1024, 2 * 1024 * 1024, DefaultChunkSize - 1, DefaultChunkSize + 1} {
			err := ValidateSnapshotChunkSize(chunkSize, useS3)
			require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())
		}
	}

	for _, chunkSize := range []uint32{8 * 1024 * 1024, 12 * 1024 * 1024, 16 * 1024 * 1024, 20 * 1024 * 1024, 32 * 1024 * 1024} {
		err := ValidateSnapshotChunkSize(chunkSize, false)
		require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())
		require.Contains(t, err.Error(), "requires S3")
		require.NoError(t, ValidateSnapshotChunkSize(chunkSize, true))
	}
}

func TestCheckDataIsAllZeroes(t *testing.T) {
	chunk := Chunk{
		Data: []byte{1, 2, 3, 4, 5},
	}
	require.False(t, chunk.CheckDataIsAllZeroes())

	chunk = Chunk{Data: []byte{0, 0, 0}}
	require.True(t, chunk.CheckDataIsAllZeroes())

	chunk = Chunk{Data: make([]byte, 1024*1024*5)}
	require.True(t, chunk.CheckDataIsAllZeroes())
}
