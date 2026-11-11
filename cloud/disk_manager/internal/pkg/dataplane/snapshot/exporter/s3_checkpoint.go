package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
)

// This is a local checkpoint, never a completion marker in the destination.
// Source data and destination parts must remain unchanged between attempts.
type s3Checkpoint struct {
	Version        uint32     `json:"version"`
	Endpoint       string     `json:"endpoint"`
	Bucket         string     `json:"bucket"`
	Prefix         string     `json:"prefix"`
	SourceIdentity string     `json:"source_identity"`
	CreateTaskID   string     `json:"create_task_id"`
	CheckpointID   string     `json:"checkpoint_id"`
	Manifest       S3Manifest `json:"manifest"`
	path           string
}

func openS3Checkpoint(
	ctx context.Context,
	destination S3Destination,
	options S3ExportOptions,
	meta storage.SnapshotMeta,
	manifest *S3Manifest,
) (*s3Checkpoint, error) {
	if options.ProgressFile == "" {
		return nil, nil
	}

	checkpoint := &s3Checkpoint{
		Version:        1,
		Endpoint:       options.Endpoint,
		Bucket:         options.Bucket,
		Prefix:         s3BackupPrefix(options.Prefix),
		SourceIdentity: options.SourceIdentity,
		CreateTaskID:   meta.CreateTaskID,
		CheckpointID:   meta.CheckpointID,
		Manifest:       *manifest,
		path:           options.ProgressFile,
	}
	file, err := os.Open(options.ProgressFile)
	if os.IsNotExist(err) {
		return checkpoint, nil
	}
	if err != nil {
		return nil, fmt.Errorf("opening export progress: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, MaxS3ManifestSize+1))
	if err != nil {
		return nil, fmt.Errorf("reading export progress: %w", err)
	}
	if len(data) > MaxS3ManifestSize {
		return nil, fmt.Errorf("export progress exceeds size limit")
	}
	var saved s3Checkpoint
	if err := json.Unmarshal(data, &saved); err != nil {
		return nil, fmt.Errorf("decoding export progress: %w", err)
	}
	// Compare all settings and metadata, excluding attempt-specific fields.
	expected := *checkpoint
	expected.path = ""
	expected.Manifest.BackupUUID = saved.Manifest.BackupUUID
	expected.Manifest.CreatedAt = saved.Manifest.CreatedAt
	expected.Manifest.Parts = nil
	identity := saved
	identity.Manifest.Parts = nil
	expectedData, err := json.Marshal(expected)
	if err != nil {
		return nil, fmt.Errorf("encoding expected export progress: %w", err)
	}
	identityData, err := json.Marshal(identity)
	if err != nil {
		return nil, fmt.Errorf("encoding saved export progress: %w", err)
	}
	if !bytes.Equal(expectedData, identityData) {
		return nil, fmt.Errorf("export progress does not match source, destination or export options")
	}

	seen := make(map[uint32]bool, len(saved.Manifest.Parts))
	for _, part := range saved.Manifest.Parts {
		if part.Number == 0 || part.Number > manifest.PartCount || seen[part.Number] {
			return nil, fmt.Errorf("invalid or duplicate export progress part %v", part.Number)
		}
		seen[part.Number] = true
		start, end, err := partitionChunkRange(meta.ChunkCount, part.Number, manifest.PartCount)
		if err != nil {
			return nil, err
		}
		length, err := partitionSize(meta.Size, start, end)
		if err != nil {
			return nil, err
		}
		if part.Offset != uint64(start)*chunkSize || part.Length != length {
			return nil, fmt.Errorf("invalid export progress range for part %v", part.Number)
		}
		if err := validateS3ManifestPart(*manifest, part); err != nil {
			return nil, err
		}
	}

	// Missing objects are uploaded again. Existing objects are trusted under
	// the same exclusive-prefix requirement as an uninterrupted export.
	parts := make([]S3ManifestPart, 0, len(saved.Manifest.Parts))
	for _, part := range saved.Manifest.Parts {
		exists, err := destination.ObjectExists(ctx, options.Bucket, checkpoint.Prefix+part.Key)
		if err != nil {
			return nil, fmt.Errorf("checking saved export part %v: %w", part.Number, err)
		}
		if exists {
			parts = append(parts, part)
		}
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Number < parts[j].Number })
	*manifest = saved.Manifest
	manifest.Parts = parts
	return checkpoint, nil
}

func (c *s3Checkpoint) save(manifest S3Manifest) error {
	if c == nil {
		return nil
	}
	c.Manifest = manifest
	data, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("encoding export progress: %w", err)
	}
	if len(data) > MaxS3ManifestSize {
		return fmt.Errorf("export progress exceeds size limit")
	}
	// Replace atomically so a crash cannot leave a truncated checkpoint.
	file, err := os.CreateTemp(filepath.Dir(c.path), ".export-progress-*")
	if err != nil {
		return fmt.Errorf("creating export progress: %w", err)
	}
	defer os.Remove(file.Name())
	_, err = file.Write(data)
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(file.Name(), c.path)
	}
	if err == nil {
		// Make the rename durable as well as the file contents.
		dir, openErr := os.Open(filepath.Dir(c.path))
		err = openErr
		if err == nil {
			err = dir.Sync()
			_ = dir.Close()
		}
	}
	if err != nil {
		return fmt.Errorf("saving export progress: %w", err)
	}
	return nil
}
