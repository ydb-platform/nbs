package dataplane

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

type exportPartsMemoryStorage struct {
	parts        map[string]map[uint32]exporter.S3ManifestPart
	deleteLimits []uint64
}

func (s *exportPartsMemoryStorage) ReadParts(
	_ context.Context,
	id string,
	after uint32,
	limit uint64,
) ([]exporter.S3ManifestPart, error) {

	var parts []exporter.S3ManifestPart
	for number, part := range s.parts[id] {
		if number > after {
			parts = append(parts, part)
		}
	}
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].Number < parts[j].Number
	})
	if uint64(len(parts)) > limit {
		parts = parts[:int(limit)]
	}
	return parts, nil
}

func (s *exportPartsMemoryStorage) UpsertParts(
	_ context.Context,
	_ *persistence.Transaction,
	id string,
	parts []exporter.S3ManifestPart,
) error {

	if s.parts[id] == nil {
		s.parts[id] = make(map[uint32]exporter.S3ManifestPart)
	}
	for _, part := range parts {
		s.parts[id][part.Number] = part
	}
	return nil
}

func (s *exportPartsMemoryStorage) DeleteParts(
	ctx context.Context,
	_ *persistence.Transaction,
	id string,
	limit uint64,
) (uint64, error) {

	s.deleteLimits = append(s.deleteLimits, limit)
	parts, err := s.ReadParts(ctx, id, 0, limit)
	for _, part := range parts {
		delete(s.parts[id], part.Number)
	}
	return uint64(len(parts)), err
}

type exportCheckpointExecutionContext struct {
	tasks.ExecutionContext
	task             *exportSnapshotTask
	id               string
	saved            []byte
	preparations     int
	failPreparation  int
	preparationError error
	inPreparation    bool
}

func (c *exportCheckpointExecutionContext) GetTaskID() string {
	if c.inPreparation {
		panic("ExecutionContext is locked during preparation")
	}
	return c.id
}

func (c *exportCheckpointExecutionContext) SaveState(context.Context) error {
	var err error
	c.saved, err = c.task.Save()
	return err
}

func (c *exportCheckpointExecutionContext) SaveStateWithPreparation(
	ctx context.Context,
	prepare func(context.Context, *persistence.Transaction) error,
) error {

	c.preparations++
	if c.preparations == c.failPreparation {
		if c.preparationError != nil {
			return c.preparationError
		}
		return errors.NewWrongGenerationError()
	}
	data, err := c.task.Save()
	if err != nil {
		return err
	}
	c.inPreparation = true
	defer func() { c.inPreparation = false }()
	if err := prepare(ctx, nil); err != nil {
		return err
	}
	c.saved = data
	return nil
}

func exportCheckpointTaskForTest() *exportSnapshotTask {
	task := &exportSnapshotTask{
		config:  &server_config.ExportConfig{},
		request: &protos.ExportSnapshotRequest{SnapshotId: "snapshot"},
		state:   &protos.ExportSnapshotTaskState{},
		parts: &exportPartsMemoryStorage{
			parts: make(map[string]map[uint32]exporter.S3ManifestPart),
		},
	}
	for number := uint32(1); number <= 1001; number++ {
		part := exporter.S3ManifestPart{
			Number: number,
			Key:    fmt.Sprintf("part-%08d.raw", number),
			Offset: uint64(number-1) * 64,
			Length: 64, StoredLength: 64, SHA256: strings.Repeat("ab", 32),
		}
		task.resume.Manifest.Parts = append(task.resume.Manifest.Parts, part)
	}
	return task
}

func TestExportPartsKeepsLegacyJSONWhenTableIsEnabled(t *testing.T) {
	ctx := context.Background()
	task := exportCheckpointTaskForTest()
	task.resume.Initialized = true
	task.config.UseExportPartsTable = proto.Bool(true)
	request, err := proto.Marshal(task.request)
	require.NoError(t, err)
	state, err := task.Save()
	require.NoError(t, err)
	resumed := &exportSnapshotTask{config: task.config}
	require.NoError(t, resumed.Load(request, state))
	execCtx := &exportCheckpointExecutionContext{task: resumed, id: "task"}
	// Legacy exports do not require the new table, including during cleanup.
	require.NoError(t, resumed.prepareExportParts(ctx, execCtx))
	require.False(t, resumed.state.PartsInYDB)
	require.Len(t, resumed.resume.Manifest.Parts, 1001)
	require.NoError(t, resumed.clearExportParts(ctx, execCtx))
	require.Zero(t, execCtx.preparations)
}

func TestExportPartsPinsNewFormatAcrossConfigChanges(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			ctx := context.Background()
			task := exportCheckpointTaskForTest()
			task.resume.Manifest.Parts = nil
			task.config.UseExportPartsTable = proto.Bool(enabled)
			execCtx := &exportCheckpointExecutionContext{task: task, id: "task"}
			require.NoError(t, task.prepareExportParts(ctx, execCtx))
			require.Equal(t, enabled, task.state.PartsInYDB)
			// Initialization pins JSON. YDB is pinned before initialization.
			task.resume.Initialized = true
			require.NoError(t, execCtx.SaveState(ctx))
			request, err := proto.Marshal(task.request)
			require.NoError(t, err)
			resumed := &exportSnapshotTask{
				parts: task.parts,
				config: &server_config.ExportConfig{
					UseExportPartsTable: proto.Bool(!enabled),
				},
			}
			require.NoError(t, resumed.Load(request, execCtx.saved))
			execCtx = &exportCheckpointExecutionContext{
				task: resumed, id: "task",
			}
			require.NoError(t, resumed.prepareExportParts(ctx, execCtx))
			require.Equal(t, enabled, resumed.state.PartsInYDB)
		})
	}
}

func TestExportPartsPinsTableBeforeInitialization(t *testing.T) {
	task := exportCheckpointTaskForTest()
	task.resume.Manifest.Parts = nil
	task.config.UseExportPartsTable = proto.Bool(true)
	execCtx := &exportCheckpointExecutionContext{task: task, id: "task"}
	require.NoError(t, task.prepareExportParts(context.Background(), execCtx))
	require.False(t, task.resume.Initialized)
	request, err := proto.Marshal(task.request)
	require.NoError(t, err)
	resumed := &exportSnapshotTask{config: &server_config.ExportConfig{}}
	require.NoError(t, resumed.Load(request, execCtx.saved))
	require.True(t, resumed.state.PartsInYDB)
	require.NoError(t, resumed.prepareExportParts(
		context.Background(), execCtx,
	))
	require.True(t, resumed.state.PartsInYDB)
}

func TestExportPublishedTaskRetriesOnlyCleanup(t *testing.T) {
	ctx := logging.SetLogger(
		context.Background(), logging.NewStderrLogger(logging.DebugLevel),
	)
	task := exportCheckpointTaskForTest()
	execCtx := &exportCheckpointExecutionContext{task: task, id: "task"}
	task.state.PartsInYDB = true
	require.NoError(t, task.parts.UpsertParts(
		ctx, nil, "task", task.resume.Manifest.Parts,
	))
	task.resume.Manifest.Parts = nil
	task.resume.Completed = true
	require.NoError(t, execCtx.SaveState(ctx))
	execCtx.preparations = 0
	execCtx.failPreparation = 2
	execCtx.preparationError = errors.NewRetriableErrorf("YDB unavailable")
	runErr := task.Run(ctx, execCtx)
	require.Error(t, runErr)
	require.True(t, errors.CanRetry(runErr))
	request, err := proto.Marshal(task.request)
	require.NoError(t, err)
	resumed := &exportSnapshotTask{parts: task.parts}
	require.NoError(t, resumed.Load(request, execCtx.saved))
	require.True(t, resumed.resume.Completed)
	execCtx = &exportCheckpointExecutionContext{
		task: resumed, id: "task",
	}
	// No source, config, or DiskService is attached: this run may only clean.
	require.NoError(t, resumed.Run(ctx, execCtx))
	parts, err := task.parts.ReadParts(ctx, "task", 0, 2000)
	require.NoError(t, err)
	require.Empty(t, parts)
}

func TestExportCancellationClearsParts(t *testing.T) {
	task := exportCheckpointTaskForTest()
	execCtx := &exportCheckpointExecutionContext{task: task, id: "task"}
	ctx := logging.SetLogger(
		context.Background(), logging.NewStderrLogger(logging.DebugLevel),
	)
	task.state.PartsInYDB = true
	task.config.PartsCleanupBatchSize = proto.Uint32(17)
	require.NoError(t, task.parts.UpsertParts(
		ctx, nil, "task", task.resume.Manifest.Parts,
	))
	task.resume.Manifest.Parts = nil
	require.NoError(t, task.Cancel(ctx, execCtx))
	stored := task.parts.(*exportPartsMemoryStorage)
	require.NotEmpty(t, stored.deleteLimits)
	for _, limit := range stored.deleteLimits {
		require.EqualValues(t, 17, limit)
	}
	parts, err := task.parts.ReadParts(ctx, "task", 0, 2000)
	require.NoError(t, err)
	require.Empty(t, parts)
}

func TestExportTaskStateSizeDoesNotGrowWithParts(t *testing.T) {
	task := exportCheckpointTaskForTest()
	task.state.PartsInYDB = true
	before, err := task.Save()
	require.NoError(t, err)
	part := task.resume.Manifest.Parts[0]
	for len(task.resume.Manifest.Parts) < 163840 {
		task.resume.Manifest.Parts = append(task.resume.Manifest.Parts, part)
	}
	after, err := task.Save()
	require.NoError(t, err)
	require.Equal(t, before, after)
	var compact exporter.S3ExportState
	require.NoError(t, json.Unmarshal(task.state.ExportState, &compact))
	require.Empty(t, compact.Manifest.Parts)
	require.Len(t, task.resume.Manifest.Parts, 163840)
}

func TestExportPartConfirmationUsesTaskTransaction(t *testing.T) {
	ctx := logging.SetLogger(
		context.Background(), logging.NewStderrLogger(logging.DebugLevel),
	)
	task := exportCheckpointTaskForTest()
	part := task.resume.Manifest.Parts[0]
	task.resume.Manifest.Parts = nil
	task.resume.Manifest.Size = part.Length
	task.resume.UploadedBytes = part.Length
	task.resume.UploadedPartCount = 1
	task.state.PartsInYDB = true
	execCtx := &exportCheckpointExecutionContext{task: task, id: "task"}
	uploads := newSnapshotExportUploadLimiter(
		2, task.state, metrics.NewEmptyRegistry(),
	)
	defer uploads.close()
	checkpoint := &snapshotExportCheckpoint{
		task: task, execCtx: execCtx, uploads: uploads,
		sourceRetries: newSnapshotExportSourceRetries(task.state, time.Minute),
	}
	require.NoError(t, checkpoint.Save(
		ctx, &task.resume, []exporter.S3ManifestPart{part},
	))
	parts, err := task.parts.ReadParts(ctx, "task", 0, 10)
	require.NoError(t, err)
	require.Equal(t, []exporter.S3ManifestPart{part}, parts)
	require.Equal(t, 1, execCtx.preparations)
	var saved protos.ExportSnapshotTaskState
	require.NoError(t, proto.Unmarshal(execCtx.saved, &saved))
	require.Equal(t, 0.99, saved.Progress)
	var state exporter.S3ExportState
	require.NoError(t, json.Unmarshal(saved.ExportState, &state))
	require.Empty(t, state.Manifest.Parts)
	require.EqualValues(t, 1, state.UploadedPartCount)
}

func TestExportLegacyCheckpointKeepsPartsInTaskState(t *testing.T) {
	ctx := context.Background()
	task := exportCheckpointTaskForTest()
	task.parts = nil
	task.resume.Initialized = true
	task.resume.Manifest.Size = 1001 * 64
	task.resume.UploadedBytes = 1001 * 64
	execCtx := &exportCheckpointExecutionContext{task: task, id: "task"}
	uploads := newSnapshotExportUploadLimiter(
		2, task.state, metrics.NewEmptyRegistry(),
	)
	defer uploads.close()
	checkpoint := &snapshotExportCheckpoint{
		task: task, execCtx: execCtx, uploads: uploads,
		sourceRetries: newSnapshotExportSourceRetries(task.state, time.Minute),
	}
	require.NoError(t, checkpoint.Save(ctx, &task.resume, nil))
	require.Zero(t, execCtx.preparations)
	var saved protos.ExportSnapshotTaskState
	require.NoError(t, proto.Unmarshal(execCtx.saved, &saved))
	require.False(t, saved.PartsInYDB)
	require.Equal(t, 0.99, saved.Progress)
	var state exporter.S3ExportState
	require.NoError(t, json.Unmarshal(saved.ExportState, &state))
	require.Len(t, state.Manifest.Parts, 1001)
}
