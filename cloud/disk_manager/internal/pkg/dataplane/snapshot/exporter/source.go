package exporter

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

// Source is the read-only subset of snapshot storage used by raw export.
// Existing storage.Storage implementations satisfy it directly.
type Source interface {
	CheckSnapshotReady(context.Context, string) (storage.SnapshotMeta, error)
	ReadChunkMap(
		context.Context,
		string,
		uint32,
	) (<-chan storage.ChunkMapEntry, <-chan error)
	ReadChunk(context.Context, *common.Chunk) error
}

type backupSource struct {
	meta     storage.SnapshotMeta
	chunkIDs []string
	chunks   common.Source
}

// Loads and validates the metadata and full map once. S3 removes the backup
// object envelope; disk ciphertext and its recorded encryption remain intact.
func NewBackupSnapshotSource(
	ctx context.Context,
	reader backup.ObjectReader,
	snapshotID string,
	diskID string,
	metrics storage_metrics.Metrics,
) (Source, error) {

	return newBackupSource(
		ctx,
		reader,
		backup.LoadOptions{
			Kind:   protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT,
			ID:     snapshotID,
			DiskID: diskID,
		},
		metrics,
	)
}

func NewBackupImageSource(
	ctx context.Context,
	reader backup.ObjectReader,
	imageID string,
	metrics storage_metrics.Metrics,
) (Source, error) {

	return newBackupSource(
		ctx,
		reader,
		backup.LoadOptions{
			Kind: protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE,
			ID:   imageID,
		},
		metrics,
	)
}

func newBackupSource(
	ctx context.Context,
	reader backup.ObjectReader,
	options backup.LoadOptions,
	metrics storage_metrics.Metrics,
) (Source, error) {

	loaded, err := backup.Load(ctx, reader, options)

	if err != nil {
		return nil, err
	}

	return &backupSource{
		meta: storage.SnapshotMeta{
			ID: loaded.Meta.ID,
			Disk: &types.Disk{
				// backup.Load verifies this ID against the snapshot metadata.
				DiskId: options.DiskID,
			},
			Size:        loaded.Meta.Size,
			StorageSize: loaded.Meta.StorageSize,
			ChunkCount:  uint32(len(loaded.ChunkIDs)),
			Ready:       true,
			Encryption: &types.EncryptionDesc{
				Mode: types.EncryptionMode(loaded.Meta.EncryptionMode),
				Key: &types.EncryptionDesc_KeyHash{
					KeyHash: loaded.Meta.EncryptionKeyHash,
				},
			},
		},

		chunkIDs: loaded.ChunkIDs,
		chunks: backup.NewBackupSource(
			reader,
			loaded.ChunkIDs,
			loaded.Meta.StorageSize,
			metrics,
		),
	}, nil
}

func (s *backupSource) CheckSnapshotReady(
	ctx context.Context,
	snapshotID string,
) (storage.SnapshotMeta, error) {

	if err := ctx.Err(); err != nil {
		return storage.SnapshotMeta{}, err
	}

	if snapshotID != s.meta.ID {
		return storage.SnapshotMeta{}, fmt.Errorf(
			"backup source contains resource %q, requested %q",
			s.meta.ID,
			snapshotID,
		)
	}

	return s.meta, nil
}

func (s *backupSource) ReadChunkMap(
	ctx context.Context,
	snapshotID string,
	start uint32,
) (<-chan storage.ChunkMapEntry, <-chan error) {

	entries := make(chan storage.ChunkMapEntry)
	errors := make(chan error, 1)
	go func() {
		defer close(entries)
		defer close(errors)

		if _, err := s.CheckSnapshotReady(ctx, snapshotID); err != nil {
			errors <- err
			return
		}

		for i := start; i < s.meta.ChunkCount; i++ {
			entry := storage.ChunkMapEntry{
				ChunkIndex: i,
				ChunkID:    s.chunkIDs[i],
				StoredInS3: true,
			}

			select {
			case entries <- entry:
			case <-ctx.Done():
				errors <- ctx.Err()
				return
			}
		}
	}()

	return entries, errors
}

func (s *backupSource) ReadChunk(
	ctx context.Context,
	chunk *common.Chunk,
) error {

	if err := ctx.Err(); err != nil {
		return err
	}

	if chunk.Index >= s.meta.ChunkCount {
		return fmt.Errorf("chunk index %v is outside backup", chunk.Index)
	}

	return s.chunks.Read(ctx, chunk)
}
