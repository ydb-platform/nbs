package backup

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/bucket"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

func putLoadTestJSON(t *testing.T, r *fakeObjectReader, key string, meta any) {
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	r.objects[key] = persistence.S3Object{Data: data}
}

func putLoadTestMap(t *testing.T, r *fakeObjectReader, ids []string) {
	data, err := proto.Marshal(&protos.BackupChunkMap{ChunkIds: ids})
	require.NoError(t, err)
	r.objects[bucket.ChunkMapKey("snapshot")] = persistence.S3Object{Data: data}
}

func TestLoadBackupPreservesMetadataAndOriginalBytes(t *testing.T) {
	for _, kind := range []protos.BackupSourceKind{
		protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT,
		protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE,
	} {
		t.Run(
			kind.String(),
			func(t *testing.T) {
				r := newFakeObjectReader()
				key := bucket.SnapshotMetaKey("disk", "snapshot")
				meta := bucket.SnapshotMeta{
					ID:          "snapshot",
					DiskID:      "disk",
					FolderID:    "folder",
					Size:        3 * ChunkSize,
					StorageSize: ChunkSize,
					EncryptionMode: uint32(
						types.EncryptionMode_ENCRYPTION_AES_XTS,
					),
					EncryptionKeyHash: []byte{1, 2, 3},
				}

				if kind == protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE {
					key = bucket.ImageMetaKey("snapshot")
				}

				// The common JSON fields have the same encoding for images.
				putLoadTestJSON(t, r, key, meta)
				ids := []string{"base-chunk", "", "incremental-chunk"}
				putLoadTestMap(t, r, ids)
				loaded, err := Load(
					newTestContext(),
					r,
					LoadOptions{
						Kind:             kind,
						ID:               "snapshot",
						DiskID:           "disk",
						ExpectedFolderID: "folder",
					},
				)

				require.NoError(t, err)
				require.Equal(t, ids, loaded.ChunkIDs)
				require.Equal(t, meta.Size, loaded.Meta.Size)
				require.Equal(t, meta.StorageSize, loaded.Meta.StorageSize)
				require.Equal(
					t,
					meta.EncryptionMode,
					loaded.Meta.EncryptionMode,
				)
				require.Equal(
					t,
					meta.EncryptionKeyHash,
					loaded.Meta.EncryptionKeyHash,
				)
				require.Equal(t, r.objects[key].Data, loaded.MetaData)
				require.Equal(
					t,
					r.objects[bucket.ChunkMapKey("snapshot")].Data,
					loaded.ChunkMapData,
				)
				require.Equal(
					t,
					[]string{key, bucket.ChunkMapKey("snapshot")},
					r.requestedKeys,
				)
			},
		)
	}
}

func TestLoadBackupValidation(t *testing.T) {
	kind := protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT
	for _, tc := range []struct {
		name   string
		change func(*bucket.SnapshotMeta, *LoadOptions)
	}{
		{
			"empty id",
			func(m *bucket.SnapshotMeta, o *LoadOptions) {
				o.ID = ""
			},
		},

		{
			"empty disk",
			func(m *bucket.SnapshotMeta, o *LoadOptions) {
				o.DiskID = ""
			},
		},

		{
			"unknown kind",
			func(m *bucket.SnapshotMeta, o *LoadOptions) {
				o.Kind = 0
			},
		},

		{
			"wrong id",
			func(m *bucket.SnapshotMeta, o *LoadOptions) {
				m.ID = "other"
			},
		},

		{"wrong disk", func(m *bucket.SnapshotMeta, o *LoadOptions) {
			m.DiskID = "other"
		}},

		{"wrong folder", func(m *bucket.SnapshotMeta, o *LoadOptions) {
			m.FolderID = "other"
		}},

		{
			"zero size",
			func(m *bucket.SnapshotMeta, o *LoadOptions) {
				m.Size = 0
			},
		},

		{
			"unaligned size",
			func(m *bucket.SnapshotMeta, o *LoadOptions) {
				m.Size++
			},
		},

		{"too many chunks", func(m *bucket.SnapshotMeta, o *LoadOptions) {
			m.Size = (math.MaxUint32 + 1) * ChunkSize
		}},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				meta := bucket.SnapshotMeta{
					ID: "snapshot", DiskID: "disk",
					FolderID: "folder", Size: ChunkSize,
				}

				options := LoadOptions{
					Kind:             kind,
					ID:               "snapshot",
					DiskID:           "disk",
					ExpectedFolderID: "folder",
				}

				tc.change(&meta, &options)
				r := newFakeObjectReader()
				putLoadTestJSON(t, r, bucket.SnapshotMetaKey("disk", "snapshot"), meta)
				_, err := Load(newTestContext(), r, options)
				require.Error(t, err)
				require.True(
					t,
					errors.Is(err, errors.NewEmptyNonRetriableError()),
				)
				require.NotContains(t, r.requestedKeys, bucket.ChunkMapKey("snapshot"))
			},
		)
	}
}

func TestLoadBackupMissingAndInvalidObjects(t *testing.T) {
	kind := protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT
	metaKey := bucket.SnapshotMetaKey("disk", "snapshot")
	mapKey := bucket.ChunkMapKey("snapshot")
	for _, tc := range []struct {
		name   string
		change func(*testing.T, *fakeObjectReader)
	}{
		{"missing metadata", func(t *testing.T, r *fakeObjectReader) {
			delete(r.objects, metaKey)
		}},

		{"malformed metadata", func(t *testing.T, r *fakeObjectReader) {
			r.objects[metaKey] = persistence.S3Object{Data: []byte("{")}
		}},

		{"missing map", func(t *testing.T, r *fakeObjectReader) {
			delete(r.objects, mapKey)
		}},

		{"malformed map", func(t *testing.T, r *fakeObjectReader) {
			r.objects[mapKey] = persistence.S3Object{Data: []byte{0xff}}
		}},

		{"short map", func(t *testing.T, r *fakeObjectReader) {
			putLoadTestMap(t, r, nil)
		}},

		{"long map", func(t *testing.T, r *fakeObjectReader) {
			putLoadTestMap(t, r, []string{"", ""})
		}},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				r := newFakeObjectReader()
				putLoadTestJSON(
					t,
					r,
					metaKey,
					bucket.SnapshotMeta{
						ID: "snapshot", DiskID: "disk", Size: ChunkSize,
					},
				)

				putLoadTestMap(t, r, []string{""})
				tc.change(t, r)
				_, err := Load(
					newTestContext(),
					r,
					LoadOptions{
						Kind:   kind,
						ID:     "snapshot",
						DiskID: "disk",
					},
				)

				require.Error(t, err)
			},
		)
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestCheckSnapshotMeta(t *testing.T) {
	ctx := context.Background()
	r := newFakeObjectReader()
	key := bucket.SnapshotMetaKey("disk", "snapshot")

	// No meta: the backup is not complete.
	err := CheckSnapshotMeta(ctx, r, "disk", "snapshot")
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	// A meta of another snapshot under the key.
	r.objects[key] = persistence.S3Object{Data: []byte(`{"id": "other"}`)}
	err = CheckSnapshotMeta(ctx, r, "disk", "snapshot")
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	// Not a meta.
	r.objects[key] = persistence.S3Object{Data: []byte("garbage")}
	err = CheckSnapshotMeta(ctx, r, "disk", "snapshot")
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	r.objects[key] = persistence.S3Object{Data: []byte(`{"id": "snapshot"}`)}
	require.NoError(t, CheckSnapshotMeta(ctx, r, "disk", "snapshot"))
}

func TestCheckImageMeta(t *testing.T) {
	ctx := context.Background()
	r := newFakeObjectReader()

	err := CheckImageMeta(ctx, r, "image")
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	r.objects[bucket.ImageMetaKey("image")] = persistence.S3Object{
		Data: []byte(`{"id": "image"}`),
	}
	require.NoError(t, CheckImageMeta(ctx, r, "image"))
}
