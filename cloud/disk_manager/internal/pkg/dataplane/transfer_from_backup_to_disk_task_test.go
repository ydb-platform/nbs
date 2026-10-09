package dataplane

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"math"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	storage_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

// These tests stop at mounting the destination disk: the transfer itself
// needs NBS and is covered by transfer_tests.

const (
	restoreTestSnapshotID = "snapshot"
	restoreTestImageID    = "image"
	restoreTestSrcDiskID  = "src-disk"
	restoreTestFolderID   = "folder"
	restoreTestZoneID     = "zone"
)

////////////////////////////////////////////////////////////////////////////////

func marshalBackupChunkMap(t *testing.T, chunkIDs []string) []byte {
	data, err := proto.Marshal(&protos.BackupChunkMap{ChunkIds: chunkIDs})
	require.NoError(t, err)
	return data
}

// Returns objects the way backup.S3 does: with the backup encryption envelope
// removed. Not thread-safe.
type fakeBackupReader struct {
	objects       map[string][]byte
	requestedKeys []string
}

func newFakeBackupReader() *fakeBackupReader {
	return &fakeBackupReader{
		objects: make(map[string][]byte),
	}
}

func (r *fakeBackupReader) GetObject(
	ctx context.Context,
	key string,
) (persistence.S3Object, error) {

	r.requestedKeys = append(r.requestedKeys, key)

	data, ok := r.objects[key]
	if !ok {
		// The same error as persistence.S3Client returns for a missing key.
		return persistence.S3Object{}, errors.NewSilentNonRetriableErrorf(
			"s3 object not found: %v",
			key,
		)
	}

	return persistence.S3Object{Data: data}, nil
}

func (r *fakeBackupReader) putJSON(t *testing.T, key string, value any) {
	data, err := json.Marshal(value)
	require.NoError(t, err)
	r.objects[key] = data
}

func (r *fakeBackupReader) putChunkMap(
	t *testing.T,
	id string,
	chunkIDs []string,
) {

	r.objects[backup.ChunkMapKey(id)] = marshalBackupChunkMap(t, chunkIDs)
}

////////////////////////////////////////////////////////////////////////////////

func newRestoreFromSnapshotRequest() *protos.TransferFromBackupToDiskRequest {
	return &protos.TransferFromBackupToDiskRequest{
		SrcKind:   protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT,
		SrcId:     restoreTestSnapshotID,
		SrcDiskId: restoreTestSrcDiskID,
		DstDisk: &types.Disk{
			ZoneId: restoreTestZoneID,
			DiskId: "dst-disk",
		},
		ExpectedFolderId: restoreTestFolderID,
	}
}

func newRestoreFromImageRequest() *protos.TransferFromBackupToDiskRequest {
	return &protos.TransferFromBackupToDiskRequest{
		SrcKind: protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE,
		SrcId:   restoreTestImageID,
		DstDisk: &types.Disk{
			ZoneId: restoreTestZoneID,
			DiskId: "dst-disk",
		},
		ExpectedFolderId: restoreTestFolderID,
	}
}

// Meta of the backup that newRestoreFromSnapshotRequest asks for.
func newRestoreSnapshotMeta(chunkCount int) backup.SnapshotMeta {
	return backup.SnapshotMeta{
		ID:          restoreTestSnapshotID,
		FolderID:    restoreTestFolderID,
		DiskID:      restoreTestSrcDiskID,
		Size:        uint64(chunkCount) * chunkSize,
		StorageSize: chunkSize,
	}
}

// Meta of the backup that newRestoreFromImageRequest asks for.
func newRestoreImageMeta(chunkCount int) backup.ImageMeta {
	return backup.ImageMeta{
		ID:          restoreTestImageID,
		FolderID:    restoreTestFolderID,
		Size:        uint64(chunkCount) * chunkSize,
		StorageSize: chunkSize,
	}
}

func newTransferFromBackupToDiskTask(
	reader backup.ObjectReader,
	nbsFactory nbs_client.Factory,
	request *protos.TransferFromBackupToDiskRequest,
) *transferFromBackupToDiskTask {

	return &transferFromBackupToDiskTask{
		config:            &config.DataplaneConfig{},
		performanceConfig: &performance_config.PerformanceConfig{},
		nbsFactory:        nbsFactory,
		backupReader:      reader,
		metrics: storage_metrics.New(
			metrics.NewEmptyRegistry(),
			"backup",
		),
		request: request,
		state:   &protos.TransferFromBackupToDiskTaskState{},
	}
}

func requireNonSilentNonRetriableError(t *testing.T, err error) {
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.False(t, errors.IsSilent(err))
}

////////////////////////////////////////////////////////////////////////////////

func TestTransferFromBackupToDiskFailsOnInvalidRequest(t *testing.T) {
	ctx := newContext()

	type request = protos.TransferFromBackupToDiskRequest

	testCases := []struct {
		name       string
		invalidate func(request *request)
	}{
		{
			name: "unknown source kind",
			invalidate: func(request *request) {
				request.SrcKind =
					protos.BackupSourceKind_BACKUP_SOURCE_KIND_UNSPECIFIED
			},
		},
		{
			name: "empty source id",
			invalidate: func(request *request) {
				request.SrcId = ""
			},
		},
		{
			name: "empty source disk id",
			invalidate: func(request *request) {
				request.SrcDiskId = ""
			},
		},
		{
			name: "no destination disk",
			invalidate: func(request *request) {
				request.DstDisk = nil
			},
		},
		{
			name: "empty destination zone id",
			invalidate: func(request *request) {
				request.DstDisk.ZoneId = ""
			},
		},
		{
			name: "empty destination disk id",
			invalidate: func(request *request) {
				request.DstDisk.DiskId = ""
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			reader := newFakeBackupReader()
			reader.putJSON(
				t,
				backup.SnapshotMetaKey(
					restoreTestSrcDiskID,
					restoreTestSnapshotID,
				),
				newRestoreSnapshotMeta(1),
			)
			reader.putChunkMap(t, restoreTestSnapshotID, []string{"chunk0"})

			request := newRestoreFromSnapshotRequest()
			testCase.invalidate(request)

			// Neither execution context nor NBS should be used.
			execCtx := tasks_mocks.NewExecutionContextMock()
			nbsFactory := nbs_mocks.NewFactoryMock()
			task := newTransferFromBackupToDiskTask(reader, nbsFactory, request)

			err := task.Run(ctx, execCtx)
			requireNonSilentNonRetriableError(t, err)
			require.Empty(t, reader.requestedKeys)
		})
	}
}

func TestTransferFromBackupToDiskFailsOnInvalidMeta(t *testing.T) {
	ctx := newContext()

	testCases := []struct {
		name       string
		invalidate func(meta *backup.SnapshotMeta)
	}{
		{
			name: "other snapshot id",
			invalidate: func(meta *backup.SnapshotMeta) {
				meta.ID = "other"
			},
		},
		{
			name: "other disk id",
			invalidate: func(meta *backup.SnapshotMeta) {
				meta.DiskID = "other"
			},
		},
		{
			name: "other folder id",
			invalidate: func(meta *backup.SnapshotMeta) {
				meta.FolderID = "other"
			},
		},
		{
			name: "zero size",
			invalidate: func(meta *backup.SnapshotMeta) {
				meta.Size = 0
			},
		},
		{
			name: "size is not a multiple of chunk size",
			invalidate: func(meta *backup.SnapshotMeta) {
				meta.Size += 4096
			},
		},
		{
			name: "chunk count does not fit into uint32",
			invalidate: func(meta *backup.SnapshotMeta) {
				meta.Size = (math.MaxUint32 + 1) * chunkSize
			},
		},
	}

	metaKey := backup.SnapshotMetaKey(
		restoreTestSrcDiskID,
		restoreTestSnapshotID,
	)

	run := func(t *testing.T, reader *fakeBackupReader) {
		reader.putChunkMap(t, restoreTestSnapshotID, []string{"chunk0"})

		execCtx := tasks_mocks.NewExecutionContextMock()
		nbsFactory := nbs_mocks.NewFactoryMock()
		task := newTransferFromBackupToDiskTask(
			reader,
			nbsFactory,
			newRestoreFromSnapshotRequest(),
		)

		err := task.Run(ctx, execCtx)
		requireNonSilentNonRetriableError(t, err)
		// Chunk map should not be read.
		require.Equal(t, []string{metaKey}, reader.requestedKeys)
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			meta := newRestoreSnapshotMeta(1)
			testCase.invalidate(&meta)

			reader := newFakeBackupReader()
			reader.putJSON(t, metaKey, meta)
			run(t, reader)
		})
	}

	t.Run("not a json", func(t *testing.T) {
		reader := newFakeBackupReader()
		reader.objects[metaKey] = []byte("{")
		run(t, reader)
	})
}

func TestTransferFromBackupToDiskFailsOnInvalidChunkMap(t *testing.T) {
	ctx := newContext()

	testCases := []struct {
		name     string
		chunkMap []byte
	}{
		{
			name:     "too short",
			chunkMap: marshalBackupChunkMap(t, []string{"chunk0"}),
		},
		{
			name: "too long",
			chunkMap: marshalBackupChunkMap(
				t,
				[]string{"chunk0", "", "chunk2"},
			),
		},
		{
			name:     "not a protobuf",
			chunkMap: []byte{0xFF},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			reader := newFakeBackupReader()
			reader.putJSON(
				t,
				backup.ImageMetaKey(restoreTestImageID),
				newRestoreImageMeta(2),
			)
			reader.objects[backup.ChunkMapKey(restoreTestImageID)] =
				testCase.chunkMap

			execCtx := tasks_mocks.NewExecutionContextMock()
			nbsFactory := nbs_mocks.NewFactoryMock()
			task := newTransferFromBackupToDiskTask(
				reader,
				nbsFactory,
				newRestoreFromImageRequest(),
			)

			err := task.Run(ctx, execCtx)
			requireNonSilentNonRetriableError(t, err)
		})
	}
}

// Backup that is not found is not an emergency, as well as snapshot that is
// not found.
func TestTransferFromBackupToDiskMissingBackupIsSilentError(t *testing.T) {
	ctx := newContext()

	reader := newFakeBackupReader()
	execCtx := tasks_mocks.NewExecutionContextMock()
	nbsFactory := nbs_mocks.NewFactoryMock()

	run := func() error {
		task := newTransferFromBackupToDiskTask(
			reader,
			nbsFactory,
			newRestoreFromSnapshotRequest(),
		)
		return task.Run(ctx, execCtx)
	}

	err := run()
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.True(t, errors.IsSilent(err))

	// Backup without the chunk map is not complete.
	reader.putJSON(
		t,
		backup.SnapshotMetaKey(restoreTestSrcDiskID, restoreTestSnapshotID),
		newRestoreSnapshotMeta(1),
	)

	err = run()
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.True(t, errors.IsSilent(err))
}

func TestTransferFromBackupToDiskPinsBackup(t *testing.T) {
	testCases := []struct {
		name    string
		request *protos.TransferFromBackupToDiskRequest
		metaKey string
		meta    any
		// Differs from meta, but passes validation as well.
		changedMeta any
	}{
		{
			name:    "snapshot",
			request: newRestoreFromSnapshotRequest(),
			metaKey: backup.SnapshotMetaKey(
				restoreTestSrcDiskID,
				restoreTestSnapshotID,
			),
			meta: newRestoreSnapshotMeta(3),
			changedMeta: func() any {
				meta := newRestoreSnapshotMeta(3)
				meta.StorageSize += 1
				return meta
			}(),
		},
		{
			name:    "image",
			request: newRestoreFromImageRequest(),
			metaKey: backup.ImageMetaKey(restoreTestImageID),
			meta:    newRestoreImageMeta(3),
			changedMeta: func() any {
				meta := newRestoreImageMeta(3)
				meta.StorageSize += 1
				return meta
			}(),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := newContext()

			chunkIDs := []string{"chunk0", "", "chunk2"}
			srcID := testCase.request.SrcId

			reader := newFakeBackupReader()
			reader.putJSON(t, testCase.metaKey, testCase.meta)
			reader.putChunkMap(t, srcID, chunkIDs)

			metaHash := sha256.Sum256(reader.objects[testCase.metaKey])
			chunkMapHash := sha256.Sum256(
				reader.objects[backup.ChunkMapKey(srcID)],
			)

			execCtx := tasks_mocks.NewExecutionContextMock()
			execCtx.On("SaveState", mock.Anything).Return(nil)
			execCtx.On("SetEstimatedInflightDuration", mock.Anything)

			// Every attempt of this test ends at mounting the disk.
			mountErr := errors.NewRetriableErrorf("nbs is not available")
			nbsFactory := nbs_mocks.NewFactoryMock()
			nbsFactory.On(
				"GetClient",
				mock.Anything,
				restoreTestZoneID,
			).Return(nil, mountErr)

			task := newTransferFromBackupToDiskTask(
				reader,
				nbsFactory,
				testCase.request,
			)

			// The first attempt should pin the backup before it gets to the
			// disk.
			err := task.Run(ctx, execCtx)
			require.Same(t, mountErr, err)
			execCtx.AssertNumberOfCalls(t, "SaveState", 1)
			nbsFactory.AssertNumberOfCalls(t, "GetClient", 1)
			require.Equal(t, metaHash[:], task.state.MetaSha256)
			require.Equal(t, chunkMapHash[:], task.state.ChunkMapSha256)
			require.EqualValues(t, len(chunkIDs), task.state.ChunkCount)
			require.Zero(t, task.state.MilestoneChunkIndex)

			// The following attempts start from the saved state.
			requestBytes, err := proto.Marshal(testCase.request)
			require.NoError(t, err)
			stateBytes, err := task.Save()
			require.NoError(t, err)

			run := func() error {
				task := newTransferFromBackupToDiskTask(reader, nbsFactory, nil)
				err := task.Load(requestBytes, stateBytes)
				require.NoError(t, err)

				return task.Run(ctx, execCtx)
			}

			// Nothing to save if the backup is the same.
			err = run()
			require.Same(t, mountErr, err)
			execCtx.AssertNumberOfCalls(t, "SaveState", 1)
			nbsFactory.AssertNumberOfCalls(t, "GetClient", 2)

			// Chunk map of the same length is still another chunk map.
			reader.putChunkMap(t, srcID, []string{"chunk0", "chunk1", "chunk2"})
			err = run()
			requireNonSilentNonRetriableError(t, err)
			require.ErrorContains(t, err, "chunk map")

			reader.putChunkMap(t, srcID, chunkIDs)
			reader.putJSON(t, testCase.metaKey, testCase.changedMeta)
			err = run()
			requireNonSilentNonRetriableError(t, err)
			require.ErrorContains(t, err, "meta")

			// The disk should not be touched after the backup has changed.
			execCtx.AssertNumberOfCalls(t, "SaveState", 1)
			nbsFactory.AssertNumberOfCalls(t, "GetClient", 2)
		})
	}
}

func TestTransferFromBackupToDiskDoesNotMountDiskIfBackupIsNotPinned(
	t *testing.T,
) {

	ctx := newContext()

	reader := newFakeBackupReader()
	reader.putJSON(
		t,
		backup.SnapshotMetaKey(restoreTestSrcDiskID, restoreTestSnapshotID),
		newRestoreSnapshotMeta(1),
	)
	reader.putChunkMap(t, restoreTestSnapshotID, []string{"chunk0"})

	saveErr := errors.NewRetriableErrorf("failed to save state")
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("SaveState", mock.Anything).Return(saveErr)

	// NBS should not be used.
	nbsFactory := nbs_mocks.NewFactoryMock()
	task := newTransferFromBackupToDiskTask(
		reader,
		nbsFactory,
		newRestoreFromSnapshotRequest(),
	)

	err := task.Run(ctx, execCtx)
	require.Same(t, saveErr, err)
	execCtx.AssertNumberOfCalls(t, "SaveState", 1)
}
