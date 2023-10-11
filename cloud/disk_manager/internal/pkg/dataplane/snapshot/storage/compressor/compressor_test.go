package compressor

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

type TestCase struct {
	name  string
	data  []byte
	codec string
}

////////////////////////////////////////////////////////////////////////////////

func sparseData(size, distance int) []byte {
	result := make([]byte, size)
	for i := 0; i < len(result); i += distance {
		result[i] = 1
	}
	return result
}

func randomData(size int, t *testing.T) []byte {
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	return data
}

func initializeCompletelyDifferent(testCase TestCase) []byte {
	decompressed := make([]byte, len(testCase.data))
	for i := range decompressed {
		// Fill with initial values which must not match the original.
		decompressed[i] = testCase.data[i] + 1
	}
	return decompressed
}

////////////////////////////////////////////////////////////////////////////////

func TestCompression(t *testing.T) {
	sizes := []int{1, 1337, 4096, 65535, 4096 * 1024}
	codecs := []string{"gzip", "lz4", "zstd", "zstd_cgo", ""}
	testCases := make([]TestCase, 0, len(sizes)*len(codecs)*3)

	for _, codec := range codecs {
		for _, size := range sizes {

			zeroData := make([]byte, size)
			testCases = append(
				testCases,
				TestCase{
					name:  fmt.Sprintf("%s_%d_%s", codec, size, "random"),
					data:  randomData(size, t),
					codec: codec,
				},
				TestCase{
					name:  fmt.Sprintf("%s_%d_%s", codec, size, "zero"),
					data:  zeroData,
					codec: codec,
				},
				TestCase{
					name:  fmt.Sprintf("%s_%d_%s", codec, size, "sparce"),
					data:  sparseData(size, 100),
					codec: codec,
				},
			)
		}
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			compressor, _ := newCompressor(testCase.codec)
			compressed, err := compressor.compress(testCase.data)
			require.NoError(t, err)

			decompressed := initializeCompletelyDifferent(testCase)
			err = compressor.decompress(compressed, decompressed)

			require.NoError(t, err)
			require.Equal(t, testCase.data, decompressed)
		})
	}
}
