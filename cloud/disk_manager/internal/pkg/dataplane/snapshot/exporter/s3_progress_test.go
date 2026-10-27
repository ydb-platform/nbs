package exporter

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
)

func TestS3ProgressTracksSuccessfulLogicalAndStoredBytes(t *testing.T) {
	progress := &s3ExportProgress{}
	progress.addRead(8 * chunkSize)
	progress.startCompression()
	require.Equal(t, 1, progress.state.activeCompression)
	progress.finishCompression()
	progress.startUpload()
	require.Zero(t, progress.state.completed)
	require.Zero(t, progress.state.uploaded)
	progress.finishUpload()
	progress.addCompleted(4*chunkSize, 123)
	require.EqualValues(t, 4*chunkSize, progress.state.completed)
	require.EqualValues(t, 123, progress.state.uploaded)
	require.EqualValues(t, 1, progress.state.completedParts)
	progress.addCompleted(2*chunkSize, 321)
	require.EqualValues(t, 6*chunkSize, progress.state.completed)
	require.EqualValues(t, 444, progress.state.uploaded)
	require.EqualValues(t, 2, progress.state.completedParts)
	require.Equal(t, 0.25, s3CompressionRatio(100, 25))
	require.Zero(t, s3CompressionRatio(0, 0))
	require.Zero(t, progress.state.activeCompression)
	require.Zero(t, progress.state.activeUploads)
}

func TestS3ProgressRecordsSuccessfulPUTDuringCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()
	progress := &s3ExportProgress{}
	dest := newS3TestDestination()
	dest.put = func(key string, data []byte) error {
		require.False(t, strings.HasSuffix(key, "manifest.json"))
		cancel() // The PUT succeeded concurrently with cancellation.
		return nil
	}

	manifest := &S3Manifest{
		SnapshotID: "snapshot",
		PartCount:  1,
	}

	options := s3TestOptions()
	options.Compression = "lz4"
	meta := storage.SnapshotMeta{
		Size:       chunkSize,
		ChunkCount: 1,
	}

	_ = exportS3Parts(
		ctx,
		&s3TestSource{meta: meta},
		dest,
		meta,
		options,
		"prefix/",
		manifest,
		progress,
		time.Now(),
		func(context.Context) error { return nil },
	)
	require.Len(t, manifest.Parts, 1)
	require.Equal(
		t,
		manifest.Parts[0].StoredLength,
		progress.state.uploaded,
	)
	require.EqualValues(t, chunkSize, progress.state.completed)
	require.EqualValues(t, 1, progress.state.completedParts)
	require.Zero(t, progress.state.activeUploads)
	require.Zero(t, progress.state.activeCompression)
	require.Zero(t, progress.state.activeReads)
	require.Zero(t, progress.state.assemblingParts)
	require.Zero(t, progress.state.readyParts)
}

func TestS3ProgressLoggerStopsAndJoins(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(newContext())
		progress := &s3ExportProgress{}
		stop := progress.startLogging(
			ctx,
			"s3://bucket/prefix/",
			0,
			0,
			time.Now(),
		)
		if canceled {
			cancel()
		}

		done := make(chan struct{})
		go func() {
			stop()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("progress logger did not join")
		}

		cancel()
	}
}
