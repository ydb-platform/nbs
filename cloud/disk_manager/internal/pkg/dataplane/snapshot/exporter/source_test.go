package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
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
) (*exportBackupReader, []byte) {

	registry := storage_metrics.New(metrics.NewEmptyRegistry(), "backup")
	r := &exportBackupReader{
		objects: make(map[string]persistence.S3Object),
		calls:   make(map[string]int),
	}

	meta, err := json.Marshal(
		backup.SnapshotMeta{
			ID:                "snapshot",
			DiskID:            "disk",
			Size:              3 * chunkSize,
			EncryptionMode:    uint32(mode),
			EncryptionKeyHash: []byte{1, 2, 3},
		},
	)

	require.NoError(t, err)
	r.objects[backup.SnapshotMetaKey("disk", "snapshot")] =
		persistence.S3Object{Data: meta}
	chunkMap, err := proto.Marshal(
		&protos.BackupChunkMap{
			ChunkIds: []string{"base-chunk", "", "incremental-chunk"},
		},
	)

	require.NoError(t, err)
	r.objects[backup.ChunkMapKey("snapshot")] =
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

func exportTestBackupSource(t *testing.T, r backup.ObjectReader) Source {
	source, err := NewBackupSnapshotSource(
		newContext(),
		r,
		"snapshot",
		"disk",
		storage_metrics.New(metrics.NewEmptyRegistry(), "backup"),
	)
	require.NoError(t, err)
	return source
}

func TestBackupExportPreservesBytesAndEncryptionMetadata(t *testing.T) {
	require.EqualValues(t, backup.ChunkSize, chunkSize)
	for _, mode := range []types.EncryptionMode{
		types.EncryptionMode_NO_ENCRYPTION,
		types.EncryptionMode_ENCRYPTION_AES_XTS,
	} {
		t.Run(
			mode.String(),
			func(t *testing.T) {
				r, data := newExportBackup(t, mode)
				source := exportTestBackupSource(t, r)
				destination := newS3TestDestination()
				manifest, err := ExportToS3(
					newContext(),
					source,
					"snapshot",
					destination,
					s3TestOptions(),
				)
				require.NoError(t, err)
				require.Equal(t, mode.String(), manifest.EncryptionMode)
				require.Equal(t, []byte{1, 2, 3}, manifest.EncryptionKeyHash)
				interpretation := "plaintext"
				if mode == types.EncryptionMode_ENCRYPTION_AES_XTS {
					interpretation = "ciphertext"
				}

				require.Equal(t, interpretation, manifest.RawDataInterpretation)
				var exported []byte
				for _, upload := range destination.uploads {
					if strings.HasSuffix(upload.key, ".raw") {
						exported = append(exported, upload.data...)
					} else {
						var decoded S3Manifest
						require.NoError(
							t,
							json.Unmarshal(upload.data, &decoded),
						)
						require.Equal(t, manifest, decoded)
						require.Contains(
							t,
							string(upload.data),
							`"encryption_key_hash": "AQID"`,
						)
					}
				}

				require.True(t, bytes.Equal(data, exported))
				// Partitions reuse the loaded map and preserve global indices.
				var partitioned bytes.Buffer
				for partition := uint32(1); partition <= 5; partition++ {
					_, err := ExportPartitionToWriterWithReadWorkers(
						newContext(),
						source,
						"snapshot",
						&partitioned,
						partition,
						5,
						2,
					)
					require.NoError(t, err)
				}

				require.True(t, bytes.Equal(data, partitioned.Bytes()))
				require.Equal(
					t,
					1,
					r.calls[backup.SnapshotMetaKey("disk", "snapshot")],
				)
				require.Equal(t, 1, r.calls[backup.ChunkMapKey("snapshot")])
				require.Zero(t, r.calls[backup.ChunkKey("")])
				_, err = source.CheckSnapshotReady(newContext(), "other")
				require.Error(t, err)
			},
		)
	}
}

func TestBackupExportChunkFailureDoesNotPublishManifest(t *testing.T) {
	for _, failure := range []string{"missing", "checksum", "compressed"} {
		t.Run(
			failure,
			func(t *testing.T) {
				r, _ := newExportBackup(t, types.EncryptionMode_NO_ENCRYPTION)
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
					exportTestBackupSource(t, r),
					"snapshot",
					destination,
					options,
				)
				require.Error(t, err)
				require.Len(t, destination.objects, 2)
				for key := range destination.objects {
					require.False(t, strings.HasSuffix(key, "manifest.json"))
				}
			},
		)
	}
}

func TestBackupExportCancellationStopsReadersWithoutManifest(t *testing.T) {
	r, _ := newExportBackup(t, types.EncryptionMode_NO_ENCRYPTION)
	source := exportTestBackupSource(t, r)
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()
	r.read = func(ctx context.Context, key string) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}

	destination := newS3TestDestination()
	_, err := ExportToS3(ctx, source, "snapshot", destination, s3TestOptions())
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
