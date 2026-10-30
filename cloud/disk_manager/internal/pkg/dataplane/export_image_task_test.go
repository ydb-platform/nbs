package dataplane

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

type imageExportBackupReader struct {
	objects map[string]persistence.S3Object
	keys    []string
}

func (r *imageExportBackupReader) GetObject(
	ctx context.Context,
	key string,
) (persistence.S3Object, error) {

	r.keys = append(r.keys, key)
	return r.objects[key], nil
}

func TestImageExportReadsImageBackupWithoutDiskID(t *testing.T) {
	meta, err := json.Marshal(backup.ImageMeta{
		ID: "image", FolderID: "folder", Size: backup.ChunkSize,
	})
	require.NoError(t, err)
	chunkMap, err := proto.Marshal(&protos.BackupChunkMap{
		ChunkIds: []string{""},
	})
	require.NoError(t, err)
	reader := &imageExportBackupReader{
		objects: map[string]persistence.S3Object{
			"images/image/meta.json": {Data: meta},
			"chunk_maps/image":       {Data: chunkMap},
		},
	}
	task := &exportSnapshotTask{
		config: &server_config.ExportConfig{
			SourceStorage: server_config.ExportConfig_BACKUP.Enum(),
		},
		request: &protos.ExportSnapshotRequest{
			SnapshotId: "image", ImageId: "image", FolderId: "folder",
		},
		state:        &protos.ExportSnapshotTaskState{},
		backupReader: reader,
		registry:     metrics.NewEmptyRegistry(),
	}
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("SaveState", mock.Anything).Return(nil).Once()
	source, err := task.selectSource(
		context.Background(), execCtx,
		newSnapshotExportSourceRetries(task.state, 30*time.Minute),
	)
	require.NoError(t, err)
	loaded, err := source.CheckSnapshotReady(context.Background(), "image")
	require.NoError(t, err)
	require.Equal(t, "image", loaded.ID)
	require.Equal(t, uint64(backup.ChunkSize), loaded.Size)
	require.Equal(t, []string{
		"images/image/meta.json", "chunk_maps/image",
	}, reader.keys)
	require.Equal(t, "backup", task.state.Source)
	require.Len(t, task.state.BackupMetaSHA256, 32)
	execCtx.AssertExpectations(t)
}
