package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

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
