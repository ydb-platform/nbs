package compressor

import (
	"bytes"
	"compress/gzip"
	"io"
	"math/rand"
	"reflect"
	"time"
	"unsafe"

	zstdcgo "github.com/DataDog/zstd"
	"github.com/pierrec/lz4"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/library/go/blockcodecs"
	_ "github.com/ydb-platform/nbs/library/go/blockcodecs/all"
)

////////////////////////////////////////////////////////////////////////////////

func compress(
	format string,
	data []byte,
	metrics metrics.Metrics,
) ([]byte, error) {

	compressor, err := newCompressor(format)
	if err != nil {
		return nil, err
	}

	before := time.Now()
	compressedData, err := compressor.compress(data)
	if err != nil {
		return nil, err
	}
	duration := time.Since(before)

	if len(format) != 0 {
		metrics.OnChunkCompressed(len(data), len(compressedData), duration, format)
	}

	return compressedData, nil
}

////////////////////////////////////////////////////////////////////////////////

func Compress(
	format string,
	data []byte,
	metrics metrics.Metrics,
	probeCompressionPercentage map[string]uint32,
) ([]byte, error) {

	compressedData, err := compress(format, data, metrics)
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	if probeCompressionPercentage == nil {
		return compressedData, nil
	}

	experimentalCodecs := []string{
		// We do not write compressed by GZIP data we just want to obtain
		// compression rate. See: NBS-3876.
		"gzip",
		// After comparing different codecs we decided to test cgo and pure-go
		// implementation of zstd. See: NBS-3164.
		"zstd",
		"zstd_cgo",
		"lz4_block",
	}

	for _, codec := range experimentalCodecs {
		compressionPercentage, ok := probeCompressionPercentage[codec]
		if !ok {
			continue
		}
		if rand.Uint32()%100 < compressionPercentage {
			_, _ = compress(codec, data, metrics)
		}
	}

	return compressedData, nil
}

func Decompress(
	format string,
	data []byte,
	result []byte,
	metrics metrics.Metrics,
) error {

	compressor, err := newCompressor(format)
	if err != nil {
		return err
	}

	before := time.Now()
	err = compressor.decompress(data, result)
	if err != nil {
		return errors.NewNonRetriableError(err)
	}
	duration := time.Since(before)

	if len(format) != 0 {
		metrics.OnChunkDecompressed(duration, format)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

type compressor interface {
	compress(data []byte) ([]byte, error)
	decompress(data []byte, result []byte) error
}

func newCompressor(format string) (compressor, error) {
	switch format {
	case "lz4":
		return &lz4Compressor{}, nil
	case "lz4_block":
		return &lz4BlockModeCompressor{
			codec: blockcodecs.FindCodecByName("lz4-hc-safe"),
		}, nil
	case "zstd":
		return &zstdCompressor{
			codec: blockcodecs.FindCodecByName("zstd_3"),
		}, nil
	case "zstd_cgo":
		return &zstdCgoCompressor{level: 3}, nil
	case "gzip":
		return &gzipCompressor{}, nil
	case "":
		return &nullCompressor{}, nil
	default:
		return nil, errors.NewNonRetriableErrorf(
			"invalid compression format: %v",
			format,
		)
	}
}

////////////////////////////////////////////////////////////////////////////////

type nullCompressor struct{}

func (c *nullCompressor) compress(data []byte) ([]byte, error) {
	return data, nil
}

func (c *nullCompressor) decompress(data []byte, result []byte) error {
	copy(result, data)
	return nil
}

////////////////////////////////////////////////////////////////////////////////

type lz4Compressor struct{}

func (c *lz4Compressor) compress(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	buffer.Grow(len(data))

	writer := lz4.NewWriter(&buffer)
	_, err := writer.Write(data)
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (c *lz4Compressor) decompress(data []byte, result []byte) error {
	reader := lz4.NewReader(bytes.NewReader(data))
	_, err := reader.Read(result)
	if err == io.EOF {
		return nil
	}
	return err
}

////////////////////////////////////////////////////////////////////////////////

type zstdCompressor struct {
	codec blockcodecs.Codec
}

func (c *zstdCompressor) compress(data []byte) ([]byte, error) {
	return c.codec.Encode(make([]byte, 0), data)
}

func (c *zstdCompressor) decompress(data []byte, result []byte) error {
	_, err := c.codec.Decode(result, data)
	return err
}

////////////////////////////////////////////////////////////////////////////////

type zstdCgoCompressor struct {
	level int
}

func (c *zstdCgoCompressor) compress(data []byte) ([]byte, error) {
	compressionLevel := c.level
	if compressionLevel <= 0 {
		compressionLevel = zstdcgo.DefaultCompression
	}
	return zstdcgo.CompressLevel(make([]byte, 0), data, compressionLevel)
}

func (c *zstdCgoCompressor) decompress(data []byte, result []byte) error {
	decompressedBytes, err := zstdcgo.Decompress(result, data)
	if err != nil {
		return err
	}

	decompressedBytesPointer := ((*reflect.SliceHeader)(unsafe.Pointer(&decompressedBytes))).Data
	resultBytesPointer := ((*reflect.SliceHeader)(unsafe.Pointer(&result))).Data
	// compare underlying bytes pointers to check if decompress did not perform
	// allocation
	if decompressedBytesPointer == resultBytesPointer {
		return nil
	}

	if len(decompressedBytes) > len(result) {
		return errors.NewNonRetriableErrorf(
			"zstd-cgo decompressed data size %d exceeds preallocated buffer's size %d\n",
			len(decompressedBytes),
			len(result),
		)
	}

	// zstdcgo.Decompress allocates a new result slice anyways if expected
	// decompression result size would exceed tbe current slice capacity.
	copy(result, decompressedBytes)
	return nil
}

////////////////////////////////////////////////////////////////////////////////

type gzipCompressor struct{}

func (g *gzipCompressor) compress(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	buffer.Grow(len(data))

	gzipWriter := gzip.NewWriter(&buffer)

	_, err := gzipWriter.Write(data)
	if err != nil {
		return nil, err
	}

	err = gzipWriter.Close()
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (g *gzipCompressor) decompress(data []byte, result []byte) error {
	gzipReader, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	_, err = io.ReadFull(gzipReader, result)
	return err
}

////////////////////////////////////////////////////////////////////////////////

type lz4BlockModeCompressor struct {
	codec blockcodecs.Codec
}

func (l lz4BlockModeCompressor) compress(data []byte) ([]byte, error) {
	return l.codec.Encode(make([]byte, 0), data)
}

func (l lz4BlockModeCompressor) decompress(data []byte, result []byte) error {
	_, err := l.codec.Decode(result, data)
	return err
}
