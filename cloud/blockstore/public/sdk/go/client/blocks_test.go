package client

import (
	"bytes"
	"fmt"
	"testing"
)

////////////////////////////////////////////////////////////////////////////////

func TestNormalizeBlocks(t *testing.T) {
	expected := [][]byte{[]byte{1, 1}, []byte{0, 0}, []byte{2, 2}}
	blockSize := len(expected[0])
	blocksCount := len(expected)

	buffers := [][]byte{[]byte{1, 1}, []byte{}, []byte{2, 2}}

	actual, err := NormalizeBlocks(uint32(blockSize), uint32(blocksCount), buffers)

	if err != nil {
		t.Error(err)
	}

	if len(expected) != len(actual) {
		err := fmt.Errorf("Size mismatch: (expected: %x, actual: %x)", len(expected), len(actual))
		t.Error(err)
		return
	}

	for i := 0; i < len(expected); i++ {
		res := bytes.Compare(expected[i], actual[i])
		if res != 0 {
			err := fmt.Errorf("(expected: %x, actual: %x)", expected[i], actual[i])
			t.Error(err)
		}
	}
}

func TestJoinBlocks(t *testing.T) {
	expected := []byte{1, 1, 0, 0, 2, 2}

	buffers := make([][]byte, 0)
	buffers = append(buffers, []byte{1, 1})
	buffers = append(buffers, []byte{})
	buffers = append(buffers, []byte{2, 2})

	blocksCount := len(buffers)
	blockSize := len(expected) / blocksCount

	destination := make([]byte, blocksCount*blockSize)
	err := JoinBlocks(uint32(blockSize), uint32(blocksCount), buffers, destination)

	if err != nil {
		t.Error(err)
	}

	res := bytes.Compare(expected, destination)
	if res != 0 {
		err := fmt.Errorf("(expected: %x, actual: %x)", expected, destination)
		t.Error(err)
	}
}
