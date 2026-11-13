package exporter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	metrics_mocks "github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
)

// Check the durable checkpoint, rather than the in-memory state of the old
// runner: concurrently finished, uncheckpointed PUTs may repeat after a crash.
func TestS3ResumeSkipsCheckpointedParts(t *testing.T) {
	source := &s3TestSource{meta: storage.SnapshotMeta{
		ID:         "snapshot",
		Ready:      true,
		Size:       4 * chunkSize,
		ChunkCount: 4,
	}}
	destination := newS3TestDestination()
	options := s3TestOptions()
	options.Compression = "none"
	state := &S3ExportState{}
	var checkpoint []byte
	_, err := ExportToS3Resumable(
		newContext(), source, "snapshot", destination, options, state,
		func(context.Context) error {
			var err error
			checkpoint, err = json.Marshal(state)
			if err != nil {
				return err
			}

			if len(state.Manifest.Parts) == 1 {
				return task_errors.NewInterruptExecutionError()
			}

			return nil
		},
	)
	require.Error(t, err)
	require.NotContains(t, destination.objects, "parent/manifest.json")

	resumed := &S3ExportState{}
	require.NoError(t, json.Unmarshal(checkpoint, resumed))
	require.Len(t, resumed.Manifest.Parts, 1)
	recordedPart := resumed.Manifest.Parts[0].Key
	manifest, err := ExportToS3Resumable(
		newContext(), source, "snapshot", destination, options, resumed,
		func(context.Context) error { return nil },
	)
	require.NoError(t, err)
	require.True(t, resumed.Completed)
	require.NoError(t, validateS3Manifest(manifest))
	var uploads int
	for _, upload := range destination.uploads {
		if upload.key == "parent/"+recordedPart {
			uploads++
		}
	}

	require.Equal(t, 1, uploads)
	require.Contains(t, destination.objects, "parent/manifest.json")
}

func TestS3ResumeDoesNotReloadPartsAfterManifestFailure(t *testing.T) {
	source := &s3TestSource{meta: storage.SnapshotMeta{
		ID:         "snapshot",
		Ready:      true,
		Size:       4 * chunkSize,
		ChunkCount: 4,
	}}
	destination := newS3TestDestination()
	options := s3TestOptions()
	options.Compression = "none"
	destination.put = func(key string, data []byte) error {
		if strings.HasSuffix(key, "manifest.json") {
			return task_errors.NewRetriableErrorf("connection lost")
		}

		return nil
	}

	state := &S3ExportState{}
	save := func(context.Context) error { return nil }
	_, err := ExportToS3Resumable(
		newContext(), source, "snapshot", destination, options, state, save,
	)
	require.Error(t, err)
	require.Len(t, state.Manifest.Parts, 2)
	require.False(t, state.Completed)
	// A crash after the last checkpoint can leave parts in completion order.
	state.Manifest.Parts[0], state.Manifest.Parts[1] =
		state.Manifest.Parts[1], state.Manifest.Parts[0]
	reads := source.mapCalls.Load()
	destination.put = nil
	registry := metrics_mocks.NewIgnoreUnknownCallsRegistryMock()
	for _, name := range []string{
		"export/readBytes", "export/uploadedBytes", "export/uploadedLogicalBytes",
	} {
		registry.GetCounter(name, nil).On("Add", mock.Anything).Maybe()
	}

	registry.GetCounter("export/uploadedParts", nil).On("Inc").Maybe()
	registry.GetCounter("export/reusedParts", nil).On("Inc").Twice()
	registry.GetCounter("export/reusedBytes", nil).
		On("Add", int64(2*chunkSize)).Twice()
	registry.GetCounter("export/completed", nil).On("Inc").Once()
	options.MetricsRegistry = registry

	_, err = ExportToS3Resumable(
		newContext(), source, "snapshot", destination, options, state, save,
	)
	require.NoError(t, err)
	require.Equal(t, reads, source.mapCalls.Load())
	require.True(t, state.Completed)
	for _, name := range []string{
		"export/readBytes", "export/uploadedBytes", "export/uploadedLogicalBytes",
	} {
		registry.GetCounter(name, nil).AssertNotCalled(t, "Add", mock.Anything)
	}

	registry.GetCounter("export/uploadedParts", nil).AssertNotCalled(t, "Inc")
	registry.AssertAllExpectations(t)
}

func TestS3ResumeRejectsChangedDestination(t *testing.T) {
	source := &s3TestSource{}
	state := &S3ExportState{
		Initialized: true,
		Bucket:      "old-bucket",
		Prefix:      "parent/",
	}
	destination := newS3TestDestination()
	_, err := ExportToS3Resumable(
		newContext(), source, "snapshot", destination, s3TestOptions(), state,
		func(context.Context) error { return fmt.Errorf("unexpected save") },
	)
	require.ErrorContains(t, err, "destination changed")
	require.Zero(t, source.readyCalls)
	require.Empty(t, destination.uploads)
}
