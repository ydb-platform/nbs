package tests

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	tasks_headers "github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type scheduledChunkFixture struct {
	ctx         context.Context
	scheduler   tasks.Scheduler
	taskStorage tasks_storage.Storage
	storage     snapshot_storage.Storage
}

func newScheduledChunkFixture(t *testing.T) scheduledChunkFixture {
	ctx := testcommon.NewContext()
	registry := tasks.NewRegistry()
	require.NoError(t, dataplane.Register(ctx, registry))
	taskStorage := testcommon.NewTaskStorage(t, ctx)
	scheduler, err := tasks.NewScheduler(ctx, registry, taskStorage,
		&tasks_config.TasksConfig{}, metrics.NewEmptyRegistry())
	require.NoError(t, err)
	config := &snapshot_config.SnapshotConfig{
		PersistenceConfig: &persistence_config.PersistenceConfig{
			Endpoint: proto.String("localhost:" + os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT")),
			Database: proto.String("/Root"),
		},
	}
	db, err := persistence.NewYDBClient(ctx, config.PersistenceConfig, metrics.NewEmptyRegistry())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close(ctx) })
	storage, err := snapshot_storage.NewStorage(config, metrics.NewEmptyRegistry(), db, nil)
	require.NoError(t, err)
	return scheduledChunkFixture{ctx, scheduler, taskStorage, storage}
}

func (f scheduledChunkFixture) run(t *testing.T, taskType string, request proto.Message) (string, proto.Message, error) {
	ctx := tasks_headers.SetIncomingIdempotencyKey(f.ctx, fmt.Sprintf("%s_%d", t.Name(), time.Now().UnixNano()))
	id, err := f.scheduler.ScheduleTask(ctx, taskType, "", request)
	require.NoError(t, err)
	response, err := f.scheduler.WaitTaskSync(f.ctx, id, 3*time.Minute)
	return id, response, err
}

func (f scheduledChunkFixture) requireFailure(t *testing.T, taskType string, request proto.Message, message string) {
	id, _, err := f.run(t, taskType, request)
	require.Error(t, err)
	require.Contains(t, err.Error(), message)
	state, err := f.taskStorage.GetTask(f.ctx, id)
	require.NoError(t, err)
	require.Zero(t, state.RetriableErrorCount)
}

func (f scheduledChunkFixture) deleteSnapshot(t *testing.T, id string) {
	_, _, err := f.run(t, "dataplane.DeleteSnapshot", &protos.DeleteSnapshotRequest{SnapshotId: id})
	require.NoError(t, err)
}

func scheduledChunkServer(t *testing.T, data []byte) (*httptest.Server, *int32) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.Header().Set("ETag", "scheduled-chunk-data")
		http.ServeContent(w, r, "disk.raw", time.Unix(1, 0), bytes.NewReader(data))
	}))
	t.Cleanup(server.Close)
	return server, &requests
}

func TestScheduledSnapshotChunkSizes(t *testing.T) {
	f := newScheduledChunkFixture(t)
	data := bytes.Repeat([]byte{0x7b}, 8*1024*1024+4096)
	server, _ := scheduledChunkServer(t, data)
	for _, tc := range []struct {
		name  string
		size  uint32
		useS3 bool
	}{
		{name: "legacy_default"},
		{name: "s3_8MiB", size: 8 * 1024 * 1024, useS3: true},
		{name: "s3_12MiB", size: 12 * 1024 * 1024, useS3: true},
		{name: "s3_32MiB", size: 32 * 1024 * 1024, useS3: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := strings.ReplaceAll(t.Name(), "/", "-")
			_, response, err := f.run(t, "dataplane.CreateSnapshotFromURL", &protos.CreateSnapshotFromURLRequest{
				SrcURL: server.URL, DstSnapshotId: id, ChunkSize: tc.size, UseS3: tc.useS3,
			})
			require.NoError(t, err)
			t.Cleanup(func() { f.deleteSnapshot(t, id) })
			meta, err := f.storage.CheckSnapshotReady(f.ctx, id)
			require.NoError(t, err)
			size := tc.size
			if size == 0 {
				size = dataplane_common.DefaultChunkSize
			}
			count := (uint64(len(data)) + uint64(size) - 1) / uint64(size)
			require.Equal(t, size, meta.ChunkSize)
			require.Equal(t, uint32(count), meta.ChunkCount)
			require.Equal(t, count*uint64(size), meta.Size)
			require.Equal(t, meta.Size, meta.StorageSize)
			require.Equal(t, &protos.CreateSnapshotFromURLResponse{
				SnapshotSize: meta.Size, SnapshotStorageSize: meta.StorageSize, TransferredDataSize: meta.Size,
			}, response)

			// Shallow copy must keep the source's layout, including S3 chunks.
			copyID := id + "-copy"
			_, _, err = f.run(t, "dataplane.CreateSnapshotFromSnapshot", &protos.CreateSnapshotFromSnapshotRequest{
				SrcSnapshotId: id, DstSnapshotId: copyID,
			})
			require.NoError(t, err)
			t.Cleanup(func() { f.deleteSnapshot(t, copyID) })
			copyMeta, err := f.storage.CheckSnapshotReady(f.ctx, copyID)
			require.NoError(t, err)
			require.Equal(t, meta.ChunkSize, copyMeta.ChunkSize)
			require.Equal(t, meta.Size, copyMeta.Size)

			nbsClient := testcommon.NewNbsTestingClient(t, f.ctx, "zone-a")
			diskID := id + "-restore"
			err = nbsClient.Create(f.ctx, nbs.CreateDiskParams{ID: diskID, BlocksCount: meta.Size / 4096,
				BlockSize: 4096, Kind: types.DiskKind_DISK_KIND_SSD})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, nbsClient.Delete(f.ctx, diskID)) })
			_, _, err = f.run(t, "dataplane.TransferFromSnapshotToDisk", &protos.TransferFromSnapshotToDiskRequest{
				SrcSnapshotId: copyID, DstDisk: &types.Disk{ZoneId: "zone-a", DiskId: diskID},
			})
			require.NoError(t, err)
			actual, err := nbsClient.CalculateCrc32(diskID, meta.Size)
			require.NoError(t, err)
			expected := make([]byte, meta.Size)
			copy(expected, data)
			require.Equal(t, crc32.ChecksumIEEE(expected), actual.Crc32)
		})
	}
}

func TestScheduledSnapshotChunkSizeValidation(t *testing.T) {
	f := newScheduledChunkFixture(t)
	server, requests := scheduledChunkServer(t, []byte("unused"))
	for _, size := range []uint32{1024 * 1024, 6 * 1024 * 1024, 8 * 1024 * 1024, 12 * 1024 * 1024, 16 * 1024 * 1024, 20 * 1024 * 1024} {
		useS3 := size < 8*1024*1024
		expectedError := "chunk size"
		if !useS3 {
			expectedError = "requires S3"
		}
		for _, source := range []string{"disk", "url"} {
			t.Run(fmt.Sprintf("%s_%d", source, size), func(t *testing.T) {
				id := strings.ReplaceAll(t.Name(), "/", "-")
				if source == "disk" {
					f.requireFailure(t, "dataplane.CreateSnapshotFromDisk", &protos.CreateSnapshotFromDiskRequest{
						SrcDisk:       &types.Disk{ZoneId: "zone-a", DiskId: "missing-disk"},
						DstSnapshotId: id, ChunkSize: size, UseS3: useS3,
					}, expectedError)
				} else {
					f.requireFailure(t, "dataplane.CreateSnapshotFromURL", &protos.CreateSnapshotFromURLRequest{
						SrcURL: server.URL, DstSnapshotId: id, ChunkSize: size, UseS3: useS3,
					}, expectedError)
				}
			})
		}
	}
	require.Zero(t, atomic.LoadInt32(requests))
}

func TestScheduledSnapshotChunkSizeMismatch(t *testing.T) {
	f := newScheduledChunkFixture(t)
	server, requests := scheduledChunkServer(t, []byte("unused"))
	for _, source := range []string{"disk", "url", "snapshot"} {
		t.Run(source, func(t *testing.T) {
			id := strings.ReplaceAll(t.Name(), "/", "-")
			_, err := f.storage.CreateSnapshot(f.ctx, snapshot_storage.SnapshotMeta{ID: id, ChunkSize: 8 * 1024 * 1024}, false)
			require.NoError(t, err)
			require.NoError(t, f.storage.SnapshotCreated(f.ctx, id, 0, 0, 0, nil))
			switch source {
			case "disk":
				diskID := id + "-disk"
				nbsClient := testcommon.NewNbsTestingClient(t, f.ctx, chunkSizeZone)
				require.NoError(t, nbsClient.Create(f.ctx, nbs.CreateDiskParams{
					ID: diskID, BlocksCount: 32 * chunkSizeMiB / 4096,
					BlockSize: 4096, Kind: types.DiskKind_DISK_KIND_SSD,
				}))
				t.Cleanup(func() { require.NoError(t, nbsClient.Delete(f.ctx, diskID)) })
				f.requireFailure(t, "dataplane.CreateSnapshotFromDisk", &protos.CreateSnapshotFromDiskRequest{
					SrcDisk: &types.Disk{ZoneId: chunkSizeZone, DiskId: diskID}, DstSnapshotId: id,
				}, "requested chunk size is 4194304")
			case "url":
				f.requireFailure(t, "dataplane.CreateSnapshotFromURL", &protos.CreateSnapshotFromURLRequest{
					SrcURL: server.URL, DstSnapshotId: id,
				}, "requested chunk size is 4194304")
			case "snapshot":
				srcID := id + "-source"
				_, err := f.storage.CreateSnapshot(f.ctx, snapshot_storage.SnapshotMeta{ID: srcID}, false)
				require.NoError(t, err)
				require.NoError(t, f.storage.SnapshotCreated(f.ctx, srcID, 0, 0, 0, nil))
				t.Cleanup(func() { f.deleteSnapshot(t, srcID) })
				f.requireFailure(t, "dataplane.CreateSnapshotFromSnapshot", &protos.CreateSnapshotFromSnapshotRequest{
					SrcSnapshotId: srcID, DstSnapshotId: id,
				}, "source snapshot")
			}
		})
	}
	require.Zero(t, atomic.LoadInt32(requests))
}

func TestScheduledIncrementalSnapshotInheritsChunkSize(t *testing.T) {
	for _, tc := range []struct {
		name          string
		parentSize    uint32
		parentUseS3   bool
		requestedSize uint32
	}{
		{name: "legacy_ydb_parent_to_s3", requestedSize: 8 * chunkSizeMiB},
		{name: "custom_s3_parent_with_legacy_request", parentSize: 8 * chunkSizeMiB, parentUseS3: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newScheduledChunkFixture(t)
			client, err := testcommon.NewClient(f.ctx)
			require.NoError(t, err)
			defer client.Close()
			diskID := chunkSizeTestID(t) + "-disk"
			nbsClient, data := chunkSizeCreateDisk(t, f.ctx, client, diskID)
			baseID := chunkSizeTestID(t) + "-base"
			request := func(id string, size uint32, useS3 bool) *protos.CreateSnapshotFromDiskRequest {
				require.NoError(t, nbsClient.CreateCheckpoint(f.ctx, nbs.CheckpointParams{
					DiskID: diskID, CheckpointID: id,
				}))
				return &protos.CreateSnapshotFromDiskRequest{
					SrcDisk:             &types.Disk{ZoneId: chunkSizeZone, DiskId: diskID},
					SrcDiskCheckpointId: id,
					DstSnapshotId:       id,
					ChunkSize:           size,
					UseS3:               useS3,
				}
			}
			_, _, err = f.run(t, "dataplane.CreateSnapshotFromDisk", request(baseID, tc.parentSize, tc.parentUseS3))
			require.NoError(t, err)
			t.Cleanup(func() { f.deleteSnapshot(t, baseID) })
			parentSize := tc.parentSize
			if parentSize == 0 {
				parentSize = dataplane_common.DefaultChunkSize
			}
			chunkSizeCheckSnapshot(t, f.ctx, baseID, parentSize, int64(len(data)), int64(len(data)))

			for i := 4 * chunkSizeMiB; i < 8*chunkSizeMiB; i++ {
				data[i] ^= 0x5a
			}
			session, err := nbsClient.MountRW(f.ctx, diskID, 0, 0, nil)
			require.NoError(t, err)
			require.NoError(t, session.Write(f.ctx, 4*chunkSizeMiB/4096, data[4*chunkSizeMiB:8*chunkSizeMiB]))
			session.Close(f.ctx)

			if tc.parentUseS3 {
				// Inheriting an 8 MiB parent cannot create new YDB chunks.
				invalidID := chunkSizeTestID(t) + "-ydb"
				f.requireFailure(t, "dataplane.CreateSnapshotFromDisk", request(invalidID, 0, false), "requires S3")
			}

			incrementalID := chunkSizeTestID(t) + "-incremental"
			_, response, err := f.run(t, "dataplane.CreateSnapshotFromDisk", request(incrementalID, tc.requestedSize, true))
			require.NoError(t, err)
			t.Cleanup(func() { f.deleteSnapshot(t, incrementalID) })
			meta := chunkSizeCheckSnapshot(t, f.ctx, incrementalID, parentSize, int64(len(data)), int64(len(data)))
			require.Equal(t, baseID, meta.BaseSnapshotID)
			require.Equal(t, baseID, meta.BaseCheckpointID)
			require.Equal(t, &protos.CreateSnapshotFromDiskResponse{
				SnapshotSize:        uint64(len(data)),
				SnapshotStorageSize: uint64(len(data)),
				TransferredDataSize: uint64(parentSize),
			}, response)
			chunkSizeCheckSnapshot(t, f.ctx, baseID, parentSize, int64(len(data)), int64(len(data)))

			restoredID := chunkSizeTestID(t) + "-restored"
			require.NoError(t, nbsClient.Create(f.ctx, nbs.CreateDiskParams{
				ID: restoredID, BlocksCount: uint64(len(data) / 4096),
				BlockSize: 4096, Kind: types.DiskKind_DISK_KIND_SSD,
			}))
			t.Cleanup(func() { require.NoError(t, nbsClient.Delete(f.ctx, restoredID)) })
			_, _, err = f.run(t, "dataplane.TransferFromSnapshotToDisk", &protos.TransferFromSnapshotToDiskRequest{
				SrcSnapshotId: incrementalID,
				DstDisk:       &types.Disk{ZoneId: chunkSizeZone, DiskId: restoredID},
			})
			require.NoError(t, err)
			actual, err := nbsClient.CalculateCrc32(restoredID, uint64(len(data)))
			require.NoError(t, err)
			require.Equal(t, crc32.ChecksumIEEE(data), actual.Crc32)
		})
	}
}
