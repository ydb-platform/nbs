package exporter

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
)

type durablePartCheckpoint struct {
	mu          sync.Mutex
	parts       map[uint32]S3ManifestPart
	state       []byte
	interrupt   bool
	batchSizes  []int
	rejectParts bool
	partSaved   chan struct{}
}

func (c *durablePartCheckpoint) ReadParts(
	_ context.Context,
	after uint32,
	limit uint64,
) ([]S3ManifestPart, error) {

	c.mu.Lock()
	defer c.mu.Unlock()
	var parts []S3ManifestPart
	for number, part := range c.parts {
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

func (c *durablePartCheckpoint) Save(
	_ context.Context,
	state *S3ExportState,
	parts []S3ManifestPart,
) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(parts) != 0 && c.rejectParts {
		return task_errors.NewRetriableErrorf("checkpoint unavailable")
	}
	for _, part := range parts {
		c.parts[part.Number] = part
	}
	compact := *state
	compact.Manifest.Parts = nil
	var err error
	c.state, err = json.Marshal(compact)
	if err != nil {
		return err
	}
	if len(parts) != 0 {
		c.batchSizes = append(c.batchSizes, len(parts))
		if c.partSaved != nil {
			close(c.partSaved)
			c.partSaved = nil
		}
		if c.interrupt {
			return task_errors.NewInterruptExecutionError()
		}
	}
	return nil
}

func TestS3ExternalCheckpointFailureDoesNotPublishManifest(t *testing.T) {
	source := &s3TestSource{meta: storage.SnapshotMeta{
		ID: "snapshot", Ready: true, Size: 4 * chunkSize, ChunkCount: 4,
	}}
	destination := newS3TestDestination()
	checkpoint := &durablePartCheckpoint{
		parts: make(map[uint32]S3ManifestPart), rejectParts: true,
	}
	options := s3TestOptions()
	options.CheckpointPartCount = 16
	_, err := ExportToS3WithCheckpoint(
		newContext(), source, "snapshot", destination, options,
		&S3ExportState{}, checkpoint,
	)
	require.Error(t, err)
	require.Empty(t, checkpoint.parts)
	require.NotContains(t, destination.objects, "parent/manifest.json")
	var resumed S3ExportState
	require.NoError(t, json.Unmarshal(checkpoint.state, &resumed))
	checkpoint.rejectParts = false
	manifest, err := ExportToS3WithCheckpoint(
		newContext(), source, "snapshot", destination, options, &resumed,
		checkpoint,
	)
	require.NoError(t, err)
	require.NoError(t, validateS3Manifest(manifest))
	// Unconfirmed PUTs must be repeated, even if the target accepted them.
	for _, part := range manifest.Parts {
		var count int
		for _, upload := range destination.uploads {
			if upload.key == "parent/"+part.Key {
				count++
			}
		}
		require.Equal(t, 2, count)
	}
}

func TestS3ExternalCheckpointResumesWithoutPartsInState(t *testing.T) {
	source := &s3TestSource{meta: storage.SnapshotMeta{
		ID: "snapshot", Ready: true, Size: 4 * chunkSize, ChunkCount: 4,
	}}
	destination := newS3TestDestination()
	checkpoint := &durablePartCheckpoint{
		parts: make(map[uint32]S3ManifestPart), interrupt: true,
	}
	options := s3TestOptions()
	options.Compression = "none"
	_, err := ExportToS3WithCheckpoint(
		newContext(), source, "snapshot", destination, options,
		&S3ExportState{}, checkpoint,
	)
	require.Error(t, err)
	require.Len(t, checkpoint.parts, 1)
	require.NotContains(t, destination.objects, "parent/manifest.json")
	var resumed S3ExportState
	require.NoError(t, json.Unmarshal(checkpoint.state, &resumed))
	require.Empty(t, resumed.Manifest.Parts)
	var reused S3ManifestPart
	for _, part := range checkpoint.parts {
		reused = part
	}
	checkpoint.interrupt = false
	manifest, err := ExportToS3WithCheckpoint(
		newContext(), source, "snapshot", destination, options, &resumed,
		checkpoint,
	)
	require.NoError(t, err)
	require.True(t, resumed.Completed)
	require.NoError(t, validateS3Manifest(manifest))
	var putCount int
	for _, upload := range destination.uploads {
		if upload.key == "parent/"+reused.Key {
			putCount++
		}
	}
	require.Equal(t, 1, putCount)
	require.Contains(t, destination.objects, "parent/manifest.json")
	require.Len(t, checkpoint.parts, int(manifest.PartCount))
	require.NoError(t, json.Unmarshal(checkpoint.state, &resumed))
	require.Empty(t, resumed.Manifest.Parts)
}

func TestS3ExternalCheckpointFlushesPartialBatchBeforeManifest(t *testing.T) {
	source := &s3TestSource{meta: storage.SnapshotMeta{
		ID: "snapshot", Ready: true, Size: 4 * chunkSize, ChunkCount: 4,
	}}
	destination := newS3TestDestination()
	checkpoint := &durablePartCheckpoint{parts: make(map[uint32]S3ManifestPart)}
	destination.put = func(key string, _ []byte) error {
		if strings.HasSuffix(key, "manifest.json") {
			checkpoint.mu.Lock()
			defer checkpoint.mu.Unlock()
			require.Len(t, checkpoint.parts, 2)
		}
		return nil
	}
	options := s3TestOptions()
	options.CheckpointPartCount = 16
	manifest, err := ExportToS3WithCheckpoint(
		newContext(), source, "snapshot", destination, options,
		&S3ExportState{}, checkpoint,
	)
	require.NoError(t, err)
	require.NoError(t, validateS3Manifest(manifest))
	var total int
	for _, size := range checkpoint.batchSizes {
		require.LessOrEqual(t, size, 16)
		total += size
	}
	require.Equal(t, 2, total)
}

func TestS3ExternalCheckpointFlushesAtConfiguredInterval(t *testing.T) {
	source := &s3TestSource{meta: storage.SnapshotMeta{
		ID: "snapshot", Ready: true, Size: 4 * chunkSize, ChunkCount: 4,
	}}
	partSaved := make(chan struct{})
	checkpoint := &durablePartCheckpoint{
		parts: make(map[uint32]S3ManifestPart), partSaved: partSaved,
	}
	ctx, cancel := context.WithTimeout(newContext(), 750*time.Millisecond)
	defer cancel()
	destination := newS3TestDestination()
	uploads := 0
	destination.put = func(key string, _ []byte) error {
		if strings.Contains(key, "part-") {
			uploads++
		}
		if uploads == 2 {
			// The export cannot finish the batch until the timer saves part one.
			select {
			case <-partSaved:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	options := s3TestOptions()
	options.Compression = "none"
	options.UploadWorkerCount = 1
	options.CheckpointPartCount = 16
	options.CheckpointInterval = 10 * time.Millisecond
	manifest, err := ExportToS3WithCheckpoint(
		ctx, source, "snapshot", destination, options,
		&S3ExportState{}, checkpoint,
	)
	require.NoError(t, err)
	require.NoError(t, validateS3Manifest(manifest))
	require.Equal(t, []int{1, 1}, checkpoint.batchSizes)
}

func TestS3ExternalCheckpointUsesConfiguredBatchSize(t *testing.T) {
	source := &s3TestSource{meta: storage.SnapshotMeta{
		ID: "snapshot", Ready: true, Size: 8 * chunkSize, ChunkCount: 8,
	}}
	checkpoint := &durablePartCheckpoint{parts: make(map[uint32]S3ManifestPart)}
	options := s3TestOptions()
	options.Compression = "none"
	options.CheckpointPartCount = 3
	options.CheckpointInterval = time.Hour
	manifest, err := ExportToS3WithCheckpoint(
		newContext(), source, "snapshot", newS3TestDestination(), options,
		&S3ExportState{}, checkpoint,
	)
	require.NoError(t, err)
	require.NoError(t, validateS3Manifest(manifest))
	require.Equal(t, []int{3, 1}, checkpoint.batchSizes)
}
