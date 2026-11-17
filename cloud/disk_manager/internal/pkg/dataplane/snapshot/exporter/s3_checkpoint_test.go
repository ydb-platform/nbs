package exporter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

func checkpointTestSource() *s3TestSource {
	return &s3TestSource{
		meta: storage.SnapshotMeta{Size: 3*chunkSize + 123, ChunkCount: 4},
		entries: []storage.ChunkMapEntry{
			{ChunkIndex: 0, ChunkID: "data-0"},
			{ChunkIndex: 2, ChunkID: "data-2"},
			{ChunkIndex: 3, ChunkID: "data-3"},
		},
	}
}

func readTestCheckpoint(t *testing.T, path string) s3Checkpoint {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var checkpoint s3Checkpoint
	require.NoError(t, json.Unmarshal(data, &checkpoint))
	return checkpoint
}

func TestS3CheckpointResume(t *testing.T) {
	for _, compression := range []string{"none", "lz4"} {
		t.Run(compression, func(t *testing.T) {
			options := s3TestOptions()
			options.Compression = compression
			options.ProgressFile = filepath.Join(t.TempDir(), "progress.json")
			options.Endpoint = "https://destination"
			options.SourceIdentity = "snapshot-storage"
			dest := newS3TestDestination()
			// Force part 2 to complete before part 1 fails, proving that
			// checkpoints can represent holes, not just a contiguous prefix.
			options.UploadWorkerCount = 2
			part2Done := make(chan struct{})
			dest.beforePut = func(ctx context.Context, key string, _ persistence.S3Object) error {
				if strings.Contains(key, "part-00000001.") {
					<-part2Done
					return fmt.Errorf("interrupted upload")
				}
				return nil
			}
			dest.put = func(key string, _ []byte) error {
				if strings.Contains(key, "part-00000002.") {
					close(part2Done)
				}
				return nil
			}
			_, err := ExportToS3(newContext(), checkpointTestSource(), "snapshot", dest, options)
			require.ErrorContains(t, err, "interrupted upload")
			checkpoint := readTestCheckpoint(t, options.ProgressFile)
			require.Len(t, checkpoint.Manifest.Parts, 1)
			require.EqualValues(t, 2, checkpoint.Manifest.Parts[0].Number)
			require.NotContains(t, dest.objects, "parent/manifest.json")

			dest.beforePut, dest.put = nil, nil
			dest.uploads = nil
			options.UploadWorkerCount = 1
			source := checkpointTestSource()
			manifest, err := ExportToS3(newContext(), source, "snapshot", dest, options)
			require.NoError(t, err)
			require.NoError(t, validateS3Manifest(manifest))
			require.Equal(t, checkpoint.Manifest.BackupUUID, manifest.BackupUUID)
			require.Equal(t, checkpoint.Manifest.CreatedAt, manifest.CreatedAt)
			require.EqualValues(t, 1, source.readCalls.Load())
			require.Len(t, dest.uploads, 2) // Missing part, then manifest.
			require.Equal(t, "parent/"+s3PartKey(1, compression), dest.uploads[0].key)
			for _, part := range manifest.Parts {
				decoded := decodeS3TestPart(t, compression, part, dest.objects["parent/"+part.Key])
				for offset, value := range decoded {
					index := uint32((part.Offset + uint64(offset)) / chunkSize)
					expected := byte(0)
					if index != 1 {
						expected = chunkDataByte(index)
					}
					if value != expected {
						t.Fatalf("wrong byte at %v", part.Offset+uint64(offset))
					}
				}
			}
			// A completed checkpoint requires no payload reads on rerun.
			source = checkpointTestSource()
			_, err = ExportToS3(newContext(), source, "snapshot", dest, options)
			require.NoError(t, err)
			require.Zero(t, source.mapCalls.Load())
			// Missing saved objects are reconstructed.
			delete(dest.objects, "parent/"+s3PartKey(2, compression))
			source = checkpointTestSource()
			_, err = ExportToS3(newContext(), source, "snapshot", dest, options)
			require.NoError(t, err)
			require.EqualValues(t, 2, source.readCalls.Load())
		})
	}
}

func TestS3CheckpointRejectsInvalidStateBeforeWrites(t *testing.T) {
	options := s3TestOptions()
	options.ProgressFile = filepath.Join(t.TempDir(), "progress.json")
	dest := newS3TestDestination()
	_, err := ExportToS3(newContext(), checkpointTestSource(), "snapshot", dest, options)
	require.NoError(t, err)
	original, err := os.ReadFile(options.ProgressFile)
	require.NoError(t, err)
	for _, change := range []string{"json", "version", "endpoint", "source", "bucket", "prefix", "compression", "part size", "snapshot", "metadata", "number", "duplicate", "range", "hash"} {
		t.Run(change, func(t *testing.T) {
			var saved s3Checkpoint
			require.NoError(t, json.Unmarshal(original, &saved))
			switch change {
			case "version":
				saved.Version++
			case "endpoint":
				saved.Endpoint = "other"
			case "source":
				saved.SourceIdentity = "other"
			case "bucket":
				saved.Bucket = "other"
			case "prefix":
				saved.Prefix = "other"
			case "compression":
				saved.Manifest.Compression = "none"
			case "part size":
				saved.Manifest.MaxPartSize *= 2
			case "snapshot":
				saved.Manifest.SnapshotID = "other"
			case "metadata":
				saved.CreateTaskID = "other"
			case "number":
				saved.Manifest.Parts[0].Number = 0
			case "duplicate":
				saved.Manifest.Parts = append(saved.Manifest.Parts, saved.Manifest.Parts[0])
			case "range":
				saved.Manifest.Parts[0].Offset++
			case "hash":
				saved.Manifest.Parts[0].SHA256 = "invalid"
			}
			data, err := json.Marshal(saved)
			require.NoError(t, err)
			if change == "json" {
				data = []byte("{")
			}
			require.NoError(t, os.WriteFile(options.ProgressFile, data, 0600))
			dest.uploads, dest.deleted = nil, nil
			source := checkpointTestSource()
			_, err = ExportToS3(newContext(), source, "snapshot", dest, options)
			require.Error(t, err)
			require.Empty(t, dest.uploads)
			require.Empty(t, dest.deleted)
			require.Zero(t, source.mapCalls.Load())
			require.Contains(t, dest.objects, "parent/manifest.json")
		})
	}
}

func TestS3CheckpointWriteFailure(t *testing.T) {
	for _, duringUpload := range []bool{false, true} {
		t.Run(fmt.Sprint(duringUpload), func(t *testing.T) {
			options := s3TestOptions()
			dir := t.TempDir()
			options.ProgressFile = filepath.Join(dir, "progress.json")
			dest := newS3TestDestination()
			if duringUpload {
				dest.put = func(key string, _ []byte) error {
					if strings.Contains(key, "part-00000001.") {
						if err := os.Remove(options.ProgressFile); err != nil {
							return err
						}
						return os.Mkdir(options.ProgressFile, 0700)
					}
					return nil
				}
			} else {
				options.ProgressFile = filepath.Join(dir, "missing", "progress.json")
				dest.objects["parent/manifest.json"] = []byte("previous backup")
			}
			_, err := ExportToS3(newContext(), checkpointTestSource(), "snapshot", dest, options)
			require.ErrorContains(t, err, "export progress")
			if duringUpload {
				require.NotContains(t, dest.objects, "parent/manifest.json")
			} else {
				require.Empty(t, dest.uploads)
				require.Empty(t, dest.deleted)
			}
		})
	}
}

func TestS3CheckpointSavesSuccessfulPUTDuringCancellation(t *testing.T) {
	options := s3TestOptions()
	options.ProgressFile = filepath.Join(t.TempDir(), "progress.json")
	source := &s3TestSource{meta: storage.SnapshotMeta{Size: chunkSize, ChunkCount: 1}}
	dest := newS3TestDestination()
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()
	dest.put = func(string, []byte) error {
		cancel()
		return nil
	}
	_, err := ExportToS3(ctx, source, "snapshot", dest, options)
	require.Error(t, err)
	require.Len(t, readTestCheckpoint(t, options.ProgressFile).Manifest.Parts, 1)
	require.NotContains(t, dest.objects, "parent/manifest.json")
	dest.put = nil
	_, err = ExportToS3(newContext(), source, "snapshot", dest, options)
	require.NoError(t, err)
	require.EqualValues(t, 1, source.mapCalls.Load())
}

func TestS3CheckpointEmptySnapshot(t *testing.T) {
	options := s3TestOptions()
	options.ProgressFile = filepath.Join(t.TempDir(), "progress.json")
	dest := newS3TestDestination()
	for i := 0; i < 2; i++ {
		manifest, err := ExportToS3(newContext(), &s3TestSource{}, "snapshot", dest, options)
		require.NoError(t, err)
		require.NoError(t, validateS3Manifest(manifest))
		require.Empty(t, readTestCheckpoint(t, options.ProgressFile).Manifest.Parts)
	}
}
