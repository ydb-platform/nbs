package exporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

func decodeS3TestPart(
	t *testing.T,
	compression string,
	part S3ManifestPart,
	stored []byte,
) []byte {

	require.EqualValues(t, part.StoredLength, len(stored))
	sum := sha256.Sum256(stored)
	require.Equal(t, part.SHA256, hex.EncodeToString(sum[:]))
	decoded := stored
	if compression == "lz4" {
		require.True(t, strings.HasSuffix(part.Key, ".raw.lz4"))
		require.Equal(t, []byte{4, 34, 77, 24}, stored[:4])
		// Content checksum enabled; maximum frame block size is 4 MiB.
		require.EqualValues(t, 4, stored[4]&4)
		require.EqualValues(t, 7, stored[5]>>4)
		input := bytes.NewReader(stored)
		var err error
		decoded, err = io.ReadAll(lz4.NewReader(input))
		require.NoError(t, err)
		require.Zero(t, input.Len())
	} else {
		require.Equal(t, "none", compression)
		require.True(t, strings.HasSuffix(part.Key, ".raw"))
	}

	require.EqualValues(t, part.Length, len(decoded))
	return decoded
}

func TestS3EncoderBoundsAndReuse(t *testing.T) {
	encoder, err := newS3PartEncoder(2 * chunkSize)
	require.NoError(t, err)
	allocation := &encoder.buffer.data[:cap(encoder.buffer.data)][0]
	random := make([]byte, 2*chunkSize)
	_, err = rand.New(rand.NewSource(42)).Read(random)
	require.NoError(t, err)
	for _, raw := range [][]byte{
		bytes.Repeat([]byte{0}, chunkSize),
		bytes.Repeat([]byte{17}, 2*chunkSize),
		random,
		random[:321],
	} {
		stored, err := encoder.encode(newContext(), raw)
		require.NoError(t, err)
		require.LessOrEqual(t, len(stored), len(raw)+s3FrameOverhead)
		require.Equal(t, 2*chunkSize+s3FrameOverhead, cap(stored))
		require.Same(t, allocation, &stored[0])
		sum := sha256.Sum256(stored)
		part := S3ManifestPart{
			Key:          "part-00000001.raw.lz4",
			Length:       uint64(len(raw)),
			StoredLength: uint64(len(stored)),
			SHA256:       hex.EncodeToString(sum[:]),
		}

		require.Equal(t, raw, decodeS3TestPart(t, "lz4", part, stored))
		if len(raw) == len(random) && bytes.Equal(raw, random) {
			require.Greater(t, len(stored), len(raw))
		} else if len(raw) >= chunkSize {
			require.Less(t, len(stored), len(raw)/100)
		}
	}

	_, err = encoder.encode(newContext(), make([]byte, 2*chunkSize+1))
	require.ErrorContains(t, err, "capacity")
	_, err = newS3PartEncoder(0)
	require.Error(t, err)
	_, err = newS3PartEncoder(math.MaxUint64)
	require.Error(t, err)
}

func TestS3FrameCapacityAndCancellation(t *testing.T) {
	buffer := &s3FrameBuffer{
		ctx:   newContext(),
		data:  make([]byte, 0, 16),
		limit: 16,
	}

	_, err := buffer.Write([]byte{1, 2})
	require.NoError(t, err)
	original := append([]byte(nil), buffer.data...)
	_, err = buffer.Write(make([]byte, 15))
	require.ErrorContains(t, err, "capacity")
	require.Equal(t, original, buffer.data)
	require.Equal(t, 16, cap(buffer.data))
	buffer.limit = 32 // Allocation capacity is enforced independently.
	_, err = buffer.Write(make([]byte, 15))
	require.Error(t, err)
	ctx, cancel := context.WithCancel(newContext())
	cancel()
	buffer.ctx = ctx
	_, err = buffer.Write([]byte{3})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, original, buffer.data)

	// Capacity failure from the actual encoder prevents a complete frame.
	buffer.ctx = newContext()
	buffer.data = buffer.data[:0]
	buffer.limit = 7
	writer := lz4.NewWriter(buffer)
	err = writeS3Frame(newContext(), writer, []byte{1})
	require.ErrorContains(t, err, "capacity")
}

type failingS3FrameWriter struct {
	writeErr error
	closeErr error
	short    bool
	writes   int
	closed   bool
	onWrite  func()
}

func (w *failingS3FrameWriter) Write(data []byte) (int, error) {
	w.writes++
	if w.onWrite != nil {
		w.onWrite()
	}

	if w.short {
		return len(data) - 1, nil
	}

	return len(data), w.writeErr
}

func (w *failingS3FrameWriter) Close() error {
	w.closed = true
	return w.closeErr
}

func TestS3FrameWriteCloseErrorsAndCancellation(t *testing.T) {
	writeErr := errors.New("encoding failed")
	closeErr := errors.New("close failed")
	for _, tc := range []string{"write", "close", "short", "cancel"} {
		t.Run(
			tc,
			func(t *testing.T) {
				ctx, cancel := context.WithCancel(newContext())
				defer cancel()
				writer := &failingS3FrameWriter{closeErr: closeErr}
				expected := closeErr
				switch tc {
				case "write":
					writer.writeErr = writeErr
					expected = writeErr
				case "short":
					writer.short = true
					expected = io.ErrShortWrite
				case "cancel":
					writer.onWrite = cancel
					expected = context.Canceled
				}

				err := writeS3Frame(ctx, writer, make([]byte, 2*chunkSize))
				require.ErrorIs(t, err, expected)
				require.True(t, writer.closed)
				if tc != "close" {
					require.Equal(t, 1, writer.writes)
				}
			},
		)
	}
}

func TestS3ManifestContract(t *testing.T) {
	valid := S3Manifest{
		FormatVersion: 1,
		Format:        "partitioned-raw-image",
		Compression:   "lz4",
		Size:          1,
		MaxPartSize:   chunkSize,
		PartCount:     1,
		Parts: []S3ManifestPart{{
			Number:       1,
			Key:          "part-00000001.raw.lz4",
			Length:       1,
			StoredLength: 20,
			SHA256:       strings.Repeat("ab", sha256.Size),
		}},
	}

	require.NoError(t, validateS3Manifest(valid))
	for _, tc := range []string{
		"version", "format", "missing compression", "unknown compression",
		"missing stored length", "stored limit", "raw stored length",
		"mode key", "unsafe key", "number", "uppercase hash", "part size",
	} {
		t.Run(
			tc,
			func(t *testing.T) {
				manifest := valid
				manifest.Parts = append([]S3ManifestPart(nil), valid.Parts...)
				switch tc {
				case "version":
					manifest.FormatVersion = 2
				case "format":
					manifest.Format = "other"
				case "missing compression":
					manifest.Compression = ""
				case "unknown compression":
					manifest.Compression = "gzip"
				case "missing stored length":
					manifest.Parts[0].StoredLength = 0
				case "stored limit":
					manifest.Parts[0].StoredLength = 2 + s3FrameOverhead
				case "raw stored length":
					manifest.Compression = "none"
					manifest.Parts[0].Key = "part-00000001.raw"
				case "mode key":
					manifest.Parts[0].Key = "part-00000001.raw"
				case "unsafe key":
					manifest.Parts[0].Key = "../part-00000001.raw.lz4"
				case "number":
					manifest.Parts[0].Number = 0
				case "uppercase hash":
					manifest.Parts[0].SHA256 = strings.ToUpper(manifest.Parts[0].SHA256)
				case "part size":
					manifest.MaxPartSize = math.MaxUint64
				}

				require.Error(t, validateS3Manifest(manifest))
			},
		)
	}

	encoded, err := marshalS3Manifest(valid, MaxS3ManifestSize)
	require.NoError(t, err)
	_, err = marshalS3Manifest(valid, len(encoded))
	require.NoError(t, err)
	_, err = marshalS3Manifest(valid, len(encoded)-1)
	require.ErrorContains(t, err, "exceeds")
}

func TestS3ExportIncompressibleAndZeroParts(t *testing.T) {
	for _, compression := range []string{"lz4", "none"} {
		t.Run(
			compression,
			func(t *testing.T) {
				raw := make([]byte, chunkSize)
				_, err := rand.New(rand.NewSource(42)).Read(raw)
				require.NoError(t, err)
				source := &s3TestSource{
					meta: storage.SnapshotMeta{
						Size:       3*chunkSize + 123,
						ChunkCount: 4,
					},
					entries: []storage.ChunkMapEntry{
						{
							ChunkIndex: 0,
							ChunkID:    "random",
						},
						{
							ChunkIndex: 3,
							ChunkID:    "tail",
						},
					},
					readHook: func(ctx context.Context, chunk *common.Chunk) error {
						copy(chunk.Data, raw)
						return nil
					},
				}

				options := s3TestOptions()
				options.PartSize = chunkSize
				options.Compression = compression
				dest := newS3TestDestination()
				manifest, err := ExportToS3(
					newContext(),
					source,
					"snapshot",
					dest,
					options,
				)
				require.NoError(t, err)
				prefix := s3BackupPrefix(options.Prefix)
				require.Len(t, manifest.Parts, 4)
				for i, part := range manifest.Parts {
					decoded := decodeS3TestPart(
						t,
						compression,
						part,
						dest.objects[prefix+part.Key],
					)
					expected := make([]byte, int(part.Length))
					if i == 0 || i == 3 {
						copy(expected, raw)
					}

					require.Equal(t, expected, decoded)
					if compression == "lz4" && i == 0 {
						require.Greater(t, part.StoredLength, part.Length)
					}
				}
			},
		)
	}
}

// The sink completes PUTs synchronously without retaining payload copies.
// This compares the full pipeline locally; remote latency is not modeled.
type s3BenchmarkDestination struct{}

func (s3BenchmarkDestination) BucketExists(
	context.Context,
	string,
) (bool, error) {

	return true, nil
}

func (s3BenchmarkDestination) ObjectExists(
	context.Context,
	string,
	string,
) (bool, error) {
	return false, nil
}

func (s3BenchmarkDestination) DeleteObject(
	context.Context,
	string,
	string,
) error {
	return errors.New("unexpected object deletion")
}

func (s3BenchmarkDestination) PutObjectWithOptions(
	context.Context,
	string,
	string,
	persistence.S3Object,
	persistence.S3PutObjectOptions,
) error {

	return nil
}

func (s3BenchmarkDestination) GetObjectWithLimit(
	context.Context,
	string,
	string,
	int64,
) ([]byte, error) {

	return nil, errors.New("unexpected manifest readback")
}

func BenchmarkS3Export(b *testing.B) {
	for _, pattern := range []string{"zeros", "repetitive", "random"} {
		for _, compression := range []string{"lz4", "none"} {
			b.Run(
				pattern+"/"+compression,
				func(b *testing.B) {
					data := make([]byte, chunkSize)
					if pattern == "repetitive" {
						for i := range data {
							data[i] = byte(i % 17)
						}
					} else if pattern == "random" {
						_, err := rand.New(rand.NewSource(42)).Read(data)
						require.NoError(b, err)
					}

					source := &s3TestSource{
						meta: storage.SnapshotMeta{
							Size:       8 * chunkSize,
							ChunkCount: 8,
						},
						readHook: func(ctx context.Context, chunk *common.Chunk) error {
							copy(chunk.Data, data)
							return nil
						},
					}

					if pattern != "zeros" {
						for i := uint32(0); i < 8; i++ {
							source.entries = append(
								source.entries,
								storage.ChunkMapEntry{
									ChunkIndex: i,
									ChunkID:    "data",
								},
							)
						}
					}

					options := s3TestOptions()
					options.UploadWorkerCount = 2
					options.Compression = compression
					ctx := logging.SetLogger(
						context.Background(),
						logging.NewStderrLogger(logging.ErrorLevel),
					)
					b.SetBytes(int64(source.meta.Size))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						manifest, err := ExportToS3(
							ctx,
							source,
							"benchmark",
							s3BenchmarkDestination{},
							options,
						)
						require.NoError(b, err)
						var stored uint64
						for _, part := range manifest.Parts {
							stored += part.StoredLength
						}

						b.ReportMetric(float64(stored), "stored-B/export")
					}
				},
			)
		}
	}
}
