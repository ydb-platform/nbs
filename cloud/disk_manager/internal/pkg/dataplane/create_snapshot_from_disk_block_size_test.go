package dataplane

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
)

func TestCreateSnapshotFromDiskPreservesBlockSizeWithoutBackups(t *testing.T) {
	for _, supplied := range []bool{false, true} {
		name := "legacy_request"
		if supplied {
			name = "size_in_request"
		}
		t.Run(name, func(t *testing.T) {
			f := newCreateSnapshotDiskZoneTest(t, "zone", false,
				&protos.CreateSnapshotFromDiskTaskState{}, nil)
			// No backup configuration is needed to preserve export metadata.
			f.execCtx.On("GetTaskID").Return("create").Twice()
			if supplied {
				f.task.request.BlockSize = 8192
			} else {
				f.storage.On("GetSnapshotMeta", f.ctx, "snapshot").
					Return((*storage.SnapshotMeta)(nil), nil).Once()
				f.nbsFactory.On("GetClient", f.ctx, "zone").
					Return(f.nbsClient, nil).Once()
				f.nbsClient.On("Describe", f.ctx, "disk").
					Return(nbs_client.DiskParams{BlockSize: 8192}, nil).Once()
			}
			f.storage.On("CreateSnapshot", f.ctx, storage.SnapshotMeta{
				BlockSize:    8192,
				ID:           "snapshot",
				Disk:         f.task.request.SrcDisk,
				CheckpointID: "checkpoint",
				CreateTaskID: "create",
			}).Return((*storage.SnapshotMeta)(nil), assert.AnError).Once()

			// Stop after the metadata write; the transfer is unrelated to the
			// block size that must survive deletion of the original disk.
			require.ErrorIs(t, f.task.run(f.ctx, f.execCtx), assert.AnError)
		})
	}
}

func TestCreateSnapshotFromDiskReadyDoesNotDescribeDeletedDisk(t *testing.T) {
	f := newCreateSnapshotDiskZoneTest(t, "zone", false,
		&protos.CreateSnapshotFromDiskTaskState{}, nil)
	f.execCtx.On("GetTaskID").Return("create").Once()
	f.storage.On("GetSnapshotMeta", f.ctx, "snapshot").
		Return(&storage.SnapshotMeta{ID: "snapshot", Ready: true}, nil).Once()

	require.NoError(t, f.task.run(f.ctx, f.execCtx))
}
