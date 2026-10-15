package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/compressor"
	storage_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

type exportBackupReader struct {
	mu      sync.Mutex
	objects map[string]persistence.S3Object
	calls   map[string]int
	read    func(context.Context, string) error
}

func (r *exportBackupReader) GetObject(
	ctx context.Context,
	key string,
) (persistence.S3Object, error) {

	if r.read != nil {
		if err := r.read(ctx, key); err != nil {
			return persistence.S3Object{}, err
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls[key]++
	object, ok := r.objects[key]
	if !ok {
		return persistence.S3Object{}, task_errors.NewSilentNonRetriableErrorf(
			"missing %v",
			key,
		)
	}

	return object, nil
}

func newExportBackup(
	t *testing.T,
	mode types.EncryptionMode,
	kind protos.BackupSourceKind,
) (*exportBackupReader, []byte) {

	registry := storage_metrics.New(metrics.NewEmptyRegistry(), "backup")
	r := &exportBackupReader{
		objects: make(map[string]persistence.S3Object),
		calls:   make(map[string]int),
	}

	metaKey := backup.SnapshotMetaKey("disk", "resource")
	var metaValue interface{} = backup.SnapshotMeta{
		ID:                "resource",
		DiskID:            "disk",
		Size:              3 * chunkSize,
		StorageSize:       2 * chunkSize,
		EncryptionMode:    uint32(mode),
		EncryptionKeyHash: []byte{1, 2, 3},
	}

	if kind == protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE {
		metaKey = backup.ImageMetaKey("resource")
		// No creation source IDs are required, including for URL imports.
		metaValue = backup.ImageMeta{
			ID:                "resource",
			Size:              3 * chunkSize,
			StorageSize:       2 * chunkSize,
			EncryptionMode:    uint32(mode),
			EncryptionKeyHash: []byte{1, 2, 3},
		}
	}

	meta, err := json.Marshal(metaValue)
	require.NoError(t, err)
	r.objects[metaKey] = persistence.S3Object{Data: meta}
	chunkMap, err := proto.Marshal(
		&protos.BackupChunkMap{
			ChunkIds: []string{"base-chunk", "", "incremental-chunk"},
		},
	)

	require.NoError(t, err)
	r.objects[backup.ChunkMapKey("resource")] =
		persistence.S3Object{Data: chunkMap}

	data := make([]byte, 3*chunkSize)
	for _, index := range []int{0, 2} {
		chunkData := data[index*chunkSize : (index+1)*chunkSize]
		for i := range chunkData {
			chunkData[i] = byte(index + 1)
		}

		compression := ""
		id := "base-chunk"
		if index == 2 {
			compression = "lz4"
			id = "incremental-chunk"
		}

		encoded, err := compressor.Compress(
			compression,
			chunkData,
			registry,
			nil,
		)
		require.NoError(t, err)
		r.objects[backup.ChunkKey(id)] = chunks.NewS3Object(
			chunks.ChunkBlob{
				Data:        encoded,
				Checksum:    common.Chunk{Data: chunkData}.Checksum(),
				Compression: compression,
			},
		)
	}

	return r, data
}

func exportTestBackupSource(
	t *testing.T,
	r backup.ObjectReader,
	kind protos.BackupSourceKind,
) Source {

	registry := storage_metrics.New(metrics.NewEmptyRegistry(), "backup")
	var source Source
	var err error
	if kind == protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE {
		source, err = NewBackupImageSource(
			newContext(),
			r,
			"resource",
			registry,
		)
	} else {
		source, err = NewBackupSnapshotSource(
			newContext(),
			r,
			"resource",
			"disk",
			registry,
		)
	}

	require.NoError(t, err)
	return source
}

func TestBackupExportPreservesBytesAndEncryptionMetadata(t *testing.T) {
	for _, kind := range []protos.BackupSourceKind{
		protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT,
		protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE,
	} {
		t.Run(
			kind.String(),
			func(t *testing.T) {
				for _, compression := range []string{"lz4", "none"} {
					t.Run(
						compression,
						func(t *testing.T) {
							testBackupExportPreservesBytesAndEncryptionMetadata(
								t,
								kind,
								compression,
							)
						},
					)
				}
			},
		)
	}
}

func testBackupExportPreservesBytesAndEncryptionMetadata(
	t *testing.T,
	kind protos.BackupSourceKind,
	compression string,
) {

	require.EqualValues(t, backup.ChunkSize, chunkSize)
	for _, mode := range []types.EncryptionMode{
		types.EncryptionMode_NO_ENCRYPTION,
		types.EncryptionMode_ENCRYPTION_AES_XTS,
	} {
		t.Run(
			mode.String(),
			func(t *testing.T) {
				r, data := newExportBackup(t, mode, kind)
				source := exportTestBackupSource(t, r, kind)
				meta, err := source.CheckSnapshotReady(newContext(), "resource")
				require.NoError(t, err)
				if kind == protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT {
					require.Equal(t, "disk", meta.Disk.GetDiskId())
				} else {
					require.Empty(t, meta.Disk.GetDiskId())
				}
				destination := newS3TestDestination()
				options := s3TestOptions()
				options.Compression = compression
				manifest, err := ExportToS3(
					newContext(),
					source,
					"resource",
					destination,
					options,
				)
				require.NoError(t, err)
				require.Equal(t, mode.String(), manifest.EncryptionMode)
				require.Equal(t, "resource", manifest.SnapshotID)
				require.EqualValues(t, 1, manifest.FormatVersion)
				require.Equal(t, "partitioned-raw-image", manifest.Format)
				meta, err = source.CheckSnapshotReady(newContext(), "resource")
				require.NoError(t, err)
				require.EqualValues(t, 2*chunkSize, meta.StorageSize)
				require.EqualValues(t, 3, meta.ChunkCount)
				require.Len(t, manifest.Parts, 2)
				require.EqualValues(t, chunkSize, manifest.Parts[1].Length)
				require.Equal(t, []byte{1, 2, 3}, manifest.EncryptionKeyHash)
				interpretation := "plaintext"
				if mode == types.EncryptionMode_ENCRYPTION_AES_XTS {
					interpretation = "ciphertext"
				}

				require.Equal(t, interpretation, manifest.RawDataInterpretation)
				var exported []byte
				prefix := s3BackupPrefix(options.Prefix)
				for _, part := range manifest.Parts {
					exported = append(
						exported,
						decodeS3TestPart(
							t,
							manifest.Compression,
							part,
							destination.objects[prefix+part.Key],
						)...,
					)
				}

				var decoded S3Manifest
				manifestUpload := destination.uploads[len(destination.uploads)-1]
				require.True(
					t,
					strings.HasSuffix(manifestUpload.key, "manifest.json"),
				)
				require.NoError(t, json.Unmarshal(manifestUpload.data, &decoded))
				require.Equal(t, manifest, decoded)
				require.Contains(
					t,
					string(manifestUpload.data),
					`"encryption_key_hash": "AQID"`,
				)

				require.True(t, bytes.Equal(data, exported))
				// Partitions reuse the loaded map and preserve global indices.
				var partitioned bytes.Buffer
				for partition := uint32(1); partition <= 5; partition++ {
					_, err := ExportPartitionToWriterWithReadWorkers(
						newContext(),
						source,
						"resource",
						&partitioned,
						partition,
						5,
						2,
					)
					require.NoError(t, err)
				}

				require.True(t, bytes.Equal(data, partitioned.Bytes()))
				metaKey := backup.SnapshotMetaKey("disk", "resource")
				if kind == protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE {
					metaKey = backup.ImageMetaKey("resource")
					require.Zero(
						t,
						r.calls[backup.SnapshotMetaKey(
							"disk",
							"resource",
						)],
					)
				}

				require.Equal(
					t,
					1,
					r.calls[metaKey],
				)
				require.Equal(t, 1, r.calls[backup.ChunkMapKey("resource")])
				require.Zero(t, r.calls[backup.ChunkKey("")])
				_, err = source.CheckSnapshotReady(newContext(), "other")
				require.Error(t, err)
			},
		)
	}
}

func TestBackupExportChunkFailureDoesNotPublishManifest(t *testing.T) {
	for _, kind := range []protos.BackupSourceKind{
		protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT,
		protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE,
	} {
		t.Run(
			kind.String(),
			func(t *testing.T) {
				testBackupExportChunkFailureDoesNotPublishManifest(t, kind)
			},
		)
	}
}

func testBackupExportChunkFailureDoesNotPublishManifest(
	t *testing.T,
	kind protos.BackupSourceKind,
) {

	for _, failure := range []string{"missing", "checksum", "compressed"} {
		t.Run(
			failure,
			func(t *testing.T) {
				r, _ := newExportBackup(
					t,
					types.EncryptionMode_NO_ENCRYPTION,
					kind,
				)
				key := backup.ChunkKey("incremental-chunk")
				object := r.objects[key]
				switch failure {
				case "missing":
					delete(r.objects, key)
				case "checksum":
					checksum := fmt.Sprint(uint32(0))
					object.Metadata["Checksum"] = &checksum
					r.objects[key] = object
				case "compressed":
					object.Data = []byte("invalid lz4")
					r.objects[key] = object
				}

				destination := newS3TestDestination()
				options := s3TestOptions()
				options.PartSize = chunkSize
				_, err := ExportToS3(
					newContext(),
					exportTestBackupSource(t, r, kind),
					"resource",
					destination,
					options,
				)
				require.Error(t, err)
				require.LessOrEqual(t, len(destination.objects), 2)
				for key := range destination.objects {
					require.False(t, strings.HasSuffix(key, "manifest.json"))
				}
			},
		)
	}
}

func TestBackupExportCancellationStopsReadersWithoutManifest(t *testing.T) {
	for _, kind := range []protos.BackupSourceKind{
		protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT,
		protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE,
	} {
		t.Run(
			kind.String(),
			func(t *testing.T) {
				testBackupExportCancellationStopsReadersWithoutManifest(t, kind)
			},
		)
	}
}

func testBackupExportCancellationStopsReadersWithoutManifest(
	t *testing.T,
	kind protos.BackupSourceKind,
) {

	r, _ := newExportBackup(t, types.EncryptionMode_NO_ENCRYPTION, kind)
	source := exportTestBackupSource(t, r, kind)
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()
	r.read = func(ctx context.Context, key string) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}

	destination := newS3TestDestination()
	_, err := ExportToS3(ctx, source, "resource", destination, s3TestOptions())
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, destination.objects)
}

func TestManifestKeyHashIsOptional(t *testing.T) {
	data, err := json.Marshal(S3Manifest{})
	require.NoError(t, err)
	require.NotContains(t, string(data), "encryption_key_hash")
	var manifest S3Manifest
	require.NoError(
		t,
		json.Unmarshal(
			[]byte(`{"encryption_mode":"NO_ENCRYPTION"}`),
			&manifest,
		),
	)
	require.Empty(t, manifest.EncryptionKeyHash)
}

func TestBackupImageSourceValidation(t *testing.T) {
	for _, failure := range []string{
		"missing metadata",
		"invalid metadata",
		"wrong ID",
		"zero size",
		"unaligned size",
		"too many chunks",
		"missing map",
		"invalid map",
		"short map",
		"long map",
	} {
		t.Run(
			failure,
			func(t *testing.T) {
				kind := protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE
				r, _ := newExportBackup(
					t,
					types.EncryptionMode_NO_ENCRYPTION,
					kind,
				)
				metaKey := backup.ImageMetaKey("resource")
				mapKey := backup.ChunkMapKey("resource")
				var meta backup.ImageMeta
				require.NoError(
					t,
					json.Unmarshal(
						r.objects[metaKey].Data,
						&meta,
					),
				)
				switch failure {
				case "missing metadata":
					delete(r.objects, metaKey)
				case "invalid metadata":
					r.objects[metaKey] = persistence.S3Object{Data: []byte("{")}
				case "wrong ID", "zero size",
					"unaligned size", "too many chunks":
					switch failure {
					case "wrong ID":
						meta.ID = "other"
					case "zero size":
						meta.Size = 0
					case "unaligned size":
						meta.Size++
					case "too many chunks":
						meta.Size = (math.MaxUint32 + 1) * chunkSize
					}

					data, err := json.Marshal(meta)
					require.NoError(t, err)
					r.objects[metaKey] = persistence.S3Object{Data: data}
				case "missing map":
					delete(r.objects, mapKey)
				case "invalid map":
					r.objects[mapKey] = persistence.S3Object{Data: []byte{0xff}}
				case "short map", "long map":
					ids := []string{"", ""}
					if failure == "long map" {
						ids = append(ids, "", "")
					}

					data, err := proto.Marshal(
						&protos.BackupChunkMap{
							ChunkIds: ids,
						},
					)
					require.NoError(t, err)
					r.objects[mapKey] = persistence.S3Object{Data: data}
				}

				// Existing snapshot metadata must never be used as a fallback.
				data, err := json.Marshal(
					backup.SnapshotMeta{
						ID:     "resource",
						DiskID: "disk",
						Size:   3 * chunkSize,
					},
				)
				require.NoError(t, err)
				snapshotKey := backup.SnapshotMetaKey("disk", "resource")
				r.objects[snapshotKey] = persistence.S3Object{Data: data}
				source, err := NewBackupImageSource(
					newContext(),
					r,
					"resource",
					storage_metrics.New(metrics.NewEmptyRegistry(), "backup"),
				)
				require.Error(t, err)
				require.Nil(t, source)
				require.Equal(t, 1, r.calls[metaKey])
				require.Zero(t, r.calls[snapshotKey])
			},
		)
	}
}

func TestBackupImageSourceIgnoresCreationSourceAndPreservesNormalizedRootKMS(
	t *testing.T,
) {

	kind := protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE
	r, data := newExportBackup(t, types.EncryptionMode_NO_ENCRYPTION, kind)
	// Root-KMS encryption at rest has already been normalized by creation.
	meta := backup.ImageMeta{
		ID:             "resource",
		Size:           3 * chunkSize,
		StorageSize:    2 * chunkSize,
		SrcDiskID:      "missing-disk",
		SrcImageID:     "missing-image",
		SrcSnapshotID:  "missing-snapshot",
		EncryptionMode: uint32(types.EncryptionMode_NO_ENCRYPTION),
	}

	encoded, err := json.Marshal(meta)
	require.NoError(t, err)
	r.objects[backup.ImageMetaKey("resource")] = persistence.S3Object{
		Data: encoded,
	}

	destination := newS3TestDestination()
	manifest, err := ExportToS3(
		newContext(),
		exportTestBackupSource(t, r, kind),
		"resource",
		destination,
		s3TestOptions(),
	)
	require.NoError(t, err)
	require.Equal(t, "NO_ENCRYPTION", manifest.EncryptionMode)
	require.Equal(t, "plaintext", manifest.RawDataInterpretation)
	require.Empty(t, manifest.EncryptionKeyHash)
	var exported []byte
	prefix := s3BackupPrefix(s3TestOptions().Prefix)
	for _, part := range manifest.Parts {
		exported = append(
			exported,
			decodeS3TestPart(
				t,
				manifest.Compression,
				part,
				destination.objects[prefix+part.Key],
			)...,
		)
	}

	require.Equal(t, data, exported)
	require.Len(t, r.calls, 4)
}

func TestBackupImageExportDestinationFailures(t *testing.T) {
	for _, failure := range []string{
		"part",
		"confirmed manifest",
		"unresolved manifest",
	} {
		t.Run(
			failure,
			func(t *testing.T) {
				kind := protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE
				r, _ := newExportBackup(
					t,
					types.EncryptionMode_NO_ENCRYPTION,
					kind,
				)
				destination := newS3TestDestination()
				destination.put = func(key string, data []byte) error {
					if failure == "part" {
						return task_errors.NewNonRetriableErrorf(
							"upload denied",
						)
					}

					if strings.HasSuffix(key, "manifest.json") {
						if failure == "confirmed manifest" {
							destination.objects[key] = data
						}

						return task_errors.NewRetriableErrorf("response lost")
					}

					return nil
				}

				_, err := ExportToS3(
					newContext(),
					exportTestBackupSource(t, r, kind),
					"resource",
					destination,
					s3TestOptions(),
				)
				if failure == "confirmed manifest" {
					require.NoError(t, err)
					require.Equal(t, 1, destination.readCalls)
					require.Len(t, destination.objects, 3)
					return
				}

				require.Error(t, err)
				if failure == "unresolved manifest" {
					require.ErrorContains(t, err, "publication is uncertain")
					require.Equal(t, 1, destination.readCalls)
				}

				for key := range destination.objects {
					require.False(t, strings.HasSuffix(key, "manifest.json"))
				}
			},
		)
	}
}

func TestS3ExportVerifiesDecodedSourceTailBeforeTrimming(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(
			fmt.Sprint(corrupt),
			func(t *testing.T) {
				r, data := newExportBackup(
					t,
					types.EncryptionMode_NO_ENCRYPTION,
					protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT,
				)
				size := 2*chunkSize + 123
				tail := append([]byte(nil), data[2*chunkSize:]...)
				checksum := common.Chunk{Data: tail}.Checksum()
				if corrupt {
					// This byte is outside the logical disk, but must be verified.
					tail[len(tail)-1] ^= 1
				}

				r.objects[backup.ChunkKey("incremental-chunk")] = chunks.NewS3Object(
					chunks.ChunkBlob{
						Data:     tail,
						Checksum: checksum,
					},
				)
				registry := storage_metrics.New(metrics.NewEmptyRegistry(), "export")
				// A partial logical disk uses the same decoded/checksummed API.
				// Backup metadata itself is restricted to full chunks by its loader.
				source := &s3TestSource{
					meta: storage.SnapshotMeta{
						Size:       uint64(size),
						ChunkCount: 3,
					},
					entries: []storage.ChunkMapEntry{
						{
							ChunkIndex: 0,
							ChunkID:    "base-chunk",
							StoredInS3: true,
						},
						{ChunkIndex: 1},
						{
							ChunkIndex: 2,
							ChunkID:    "incremental-chunk",
							StoredInS3: true,
						},
					},
					readHook: func(ctx context.Context, chunk *common.Chunk) error {
						object, err := r.GetObject(ctx, backup.ChunkKey(chunk.ID))
						if err != nil {
							return err
						}

						return chunks.DecodeS3Chunk(ctx, object, chunk, registry)
					},
				}

				destination := newS3TestDestination()
				options := s3TestOptions()
				manifest, err := ExportToS3(
					newContext(),
					source,
					"resource",
					destination,
					options,
				)
				if corrupt {
					require.Error(t, err)
					for key := range destination.objects {
						require.False(t, strings.HasSuffix(key, "manifest.json"))
					}

					return
				}

				require.NoError(t, err)
				prefix := s3BackupPrefix(options.Prefix)
				var decoded []byte
				for _, part := range manifest.Parts {
					decoded = append(
						decoded,
						decodeS3TestPart(
							t,
							manifest.Compression,
							part,
							destination.objects[prefix+part.Key],
						)...,
					)
				}

				require.Equal(t, data[:size], decoded)
			},
		)
	}
}
