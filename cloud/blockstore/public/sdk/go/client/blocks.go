package client

import (
	"errors"
)

////////////////////////////////////////////////////////////////////////////////

func NormalizeBlocks(
	blockSize uint32,
	blocksCount uint32,
	buffers [][]byte,
) ([][]byte, error) {

	result := make([][]byte, 0, blocksCount)
	for _, buffer := range buffers {
		if len(buffer) == 0 {
			result = append(result, make([]byte, blockSize))
			continue
		}

		for len(buffer) > 0 {
			if len(buffer) < int(blockSize) {
				return nil, errors.New("Invalid result buffer size")
			}
			result = append(result, buffer[:blockSize])
			buffer = buffer[blockSize:]
		}
	}
	return result, nil
}

func JoinBlocks(
	blockSize uint32,
	blocksCount uint32,
	buffers [][]byte,
	destination []byte,
) error {

	if len(destination) != int(blockSize*blocksCount) {
		return errors.New("Invalid destination buffer size")
	}

	index := 0
	bufLen := int(blockSize)

	for _, buffer := range buffers {
		if len(buffer) != 0 {
			if len(buffer) != bufLen {
				return errors.New("Invalid result buffer size")
			}
			copy(destination[index:index+bufLen], buffer)
		} else {
			sliceToClear := destination[index : index+bufLen]
			for i := range sliceToClear {
				sliceToClear[i] = 0
			}
		}

		index += bufLen
	}

	return nil
}

func AllBlocksEmpty(buffers [][]byte) bool {
	for _, buffer := range buffers {
		if len(buffer) != 0 {
			return false
		}
	}
	return true
}
