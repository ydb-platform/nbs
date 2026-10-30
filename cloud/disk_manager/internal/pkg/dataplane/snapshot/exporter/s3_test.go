package exporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

type s3TestSource struct {
	storage.Storage // Unexpected calls (especially mutations) fail the test.
	meta            storage.SnapshotMeta
	entries         []storage.ChunkMapEntry
	readyErr        error
	mapErr          error
	readErr         error
	readyCalls      int
	mapCalls        atomic.Int32
	readCalls       atomic.Int32
	activeReads     atomic.Int32
	blockReads      bool
	readStarted     chan struct{}
	readHook        func(context.Context, *common.Chunk) error
}

func (s *s3TestSource) CheckSnapshotReady(
	context.Context,
	string,
) (storage.SnapshotMeta, error) {

	s.readyCalls++
	return s.meta, s.readyErr
}

func (s *s3TestSource) ReadChunkMap(
	ctx context.Context,
	snapshotID string,
	start uint32,
) (<-chan storage.ChunkMapEntry, <-chan error) {

	s.mapCalls.Add(1)
	return newChunkMapChannels(
		filterPartitionEntriesFrom(s.entries, start),
		s.mapErr,
	)
}

func (s *s3TestSource) ReadChunk(
	ctx context.Context,
	chunk *common.Chunk,
) error {

	s.readCalls.Add(1)
	s.activeReads.Add(1)
	defer s.activeReads.Add(-1)
	if s.blockReads {
		s.readStarted <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}

	if s.readHook != nil {
		return s.readHook(ctx, chunk)
	}

	for i := range chunk.Data {
		chunk.Data[i] = chunkDataByte(chunk.Index)
	}

	return s.readErr
}

type s3TestUpload struct {
	key     string
	data    []byte
	options persistence.S3PutObjectOptions
}

type s3TestDestination struct {
	mu           sync.Mutex
	exists       bool
	bucketErr    error
	existsErr    error
	checkedKeys  []string
	deleteErr    error
	deleted      []string
	uploads      []s3TestUpload
	objects      map[string][]byte
	put          func(string, []byte) error
	beforePut    func(context.Context, string, persistence.S3Object) error
	readErr      error
	readMismatch bool
	readCalls    int
}

func newS3TestDestination() *s3TestDestination {
	return &s3TestDestination{
		exists:  true,
		objects: make(map[string][]byte),
	}
}

func (d *s3TestDestination) BucketExists(
	context.Context,
	string,
) (bool, error) {

	return d.exists, d.bucketErr
}

func (d *s3TestDestination) ObjectExists(
	ctx context.Context,
	bucket string,
	key string,
) (bool, error) {

	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}

	if d.existsErr != nil {
		return false, d.existsErr
	}

	d.checkedKeys = append(d.checkedKeys, key)
	_, exists := d.objects[key]
	return exists, nil
}

func (d *s3TestDestination) DeleteObject(
	ctx context.Context,
	bucket string,
	key string,
) error {

	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	if d.deleteErr != nil {
		return d.deleteErr
	}

	delete(d.objects, key)
	d.deleted = append(d.deleted, key)
	return nil
}

func (d *s3TestDestination) PutObjectWithOptions(
	ctx context.Context,
	bucket string,
	key string,
	object persistence.S3Object,
	options persistence.S3PutObjectOptions,
) error {

	if err := ctx.Err(); err != nil {
		return err
	}

	if d.beforePut != nil {
		if err := d.beforePut(ctx, key, object); err != nil {
			return err
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	data := append([]byte(nil), object.Data...)
	d.mu.Lock()
	d.uploads = append(d.uploads, s3TestUpload{key, data, options})
	d.mu.Unlock()
	if d.put != nil {
		if err := d.put(key, data); err != nil {
			return err
		}
	}

	d.mu.Lock()
	d.objects[key] = data
	d.mu.Unlock()
	return nil
}

func (d *s3TestDestination) GetObjectWithLimit(
	ctx context.Context,
	bucket string,
	key string,
	limit int64,
) ([]byte, error) {

	d.mu.Lock()
	defer d.mu.Unlock()
	d.readCalls++
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if d.readErr != nil {
		return nil, d.readErr
	}

	if d.readMismatch {
		return []byte("different"), nil
	}

	data := d.objects[key]
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("read limit exceeded")
	}

	return data, nil
}

func s3TestOptions() S3ExportOptions {
	return S3ExportOptions{
		Bucket:            "destination",
		Prefix:            "/parent/",
		PartSize:          2 * chunkSize,
		ReadWorkerCount:   2,
		UploadWorkerCount: 1,
	}
}

func TestS3PartitionPlanning(t *testing.T) {
	for _, tc := range []struct {
		name     string
		size     uint64
		chunks   uint32
		partSize uint64
		count    uint32
		invalid  bool
	}{
		{"empty", 0, 0, chunkSize, 0, false},
		{"partial", 7, 1, chunkSize, 1, false},
		{"exact", 4 * chunkSize, 4, 2 * chunkSize, 2, false},
		{"balanced", 5*chunkSize - 1, 5, 2 * chunkSize, 3, false},
		{"max part", 2*MaxS3PartSize + 1, 513, MaxS3PartSize, 3, false},
		{
			"max chunks",
			uint64(math.MaxUint32) * chunkSize,
			math.MaxUint32,
			chunkSize,
			math.MaxUint32,
			false,
		},

		{"zero part", 0, 0, 0, 0, true},
		{"unaligned part", 1, 1, chunkSize + 1, 0, true},
		{"oversized part", 1, 1, MaxS3PartSize + chunkSize, 0, true},
		{"overflow size", math.MaxUint64, math.MaxUint32, chunkSize, 0, true},
		{
			"too many chunks",
			uint64(math.MaxUint32)*chunkSize + 1,
			math.MaxUint32,
			chunkSize,
			0,
			true,
		},

		{"missing chunks", chunkSize + 1, 1, chunkSize, 0, true},
		{"extra chunks", chunkSize, 2, chunkSize, 0, true},
		{"invalid empty", 0, 1, chunkSize, 0, true},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				count, err := s3PartitionCount(
					storage.SnapshotMeta{
						Size:       tc.size,
						ChunkCount: tc.chunks,
					},
					tc.partSize,
				)
				if tc.invalid {
					require.Error(t, err)
					return
				}

				require.NoError(t, err)
				require.Equal(t, tc.count, count)
				if count == 0 {
					return
				}

				for _, number := range []uint32{1, (count-1)/2 + 1, count} {
					start, end, err := partitionChunkRange(tc.chunks, number, count)
					require.NoError(t, err)
					length, err := partitionSize(tc.size, start, end)
					require.NoError(t, err)
					require.Positive(t, length)
					require.LessOrEqual(t, length, tc.partSize)
				}
			},
		)
	}

	require.Error(t, ValidateReadWorkers(math.MaxInt))
}

func TestS3ExportReconstructionAndDirectPrefixes(t *testing.T) {
	for _, compression := range []string{"", "lz4", "none"} {
		t.Run(
			compression,
			func(t *testing.T) {
				testS3ExportReconstruction(t, compression)
			},
		)
	}
}

func testS3ExportReconstruction(t *testing.T, compression string) {
	source := &s3TestSource{
		meta: storage.SnapshotMeta{
			Size:       4*chunkSize + 123,
			ChunkCount: 5,
		},
		entries: []storage.ChunkMapEntry{
			{
				ChunkIndex: 0,
				ChunkID:    "data-0",
			},
			{ChunkIndex: 1},
			{
				ChunkIndex: 2,
				ChunkID:    "data-2",
				StoredInS3: true,
			},
			{
				ChunkIndex: 4,
				ChunkID:    "data-4",
			},
		},
	}

	dest := newS3TestDestination()
	options := s3TestOptions()
	options.Compression = compression
	for invocation := 0; invocation < 3; invocation++ {
		if invocation == 1 {
			options.Prefix = ""
		} else {
			options.Prefix = "/parent/"
		}

		before := len(dest.uploads)
		manifest, err := ExportToS3(newContext(), source, "snapshot", dest, options)
		require.NoError(t, err)
		require.Equal(t, uint32(3), manifest.PartCount)
		require.Equal(t, "partitioned-raw-image", manifest.Format)
		require.Equal(t, uint32(1), manifest.FormatVersion)
		require.NoError(t, validateS3Manifest(manifest))
		prefix := s3BackupPrefix(options.Prefix)
		if invocation != 1 {
			require.Equal(t, "parent/", prefix)
		} else {
			require.Empty(t, prefix)
		}

		var reconstructed bytes.Buffer
		uploadsByKey := make(map[string]s3TestUpload)
		for _, upload := range dest.uploads[before:] {
			uploadsByKey[upload.key] = upload
		}

		for _, part := range manifest.Parts {
			data := dest.objects[prefix+part.Key]
			require.Equal(t, part.StoredLength, uint64(len(data)))
			require.Equal(t, part.Offset, uint64(reconstructed.Len()))
			sum := sha256.Sum256(data)
			require.Equal(t, part.SHA256, hex.EncodeToString(sum[:]))
			reconstructed.Write(
				decodeS3TestPart(t, manifest.Compression, part, data),
			)
			upload, exists := uploadsByKey[prefix+part.Key]
			require.True(t, exists)
			require.Equal(
				t,
				"application/octet-stream",
				upload.options.ContentType,
			)
			require.True(t, upload.options.ContentMD5)
			require.Equal(t, part.SHA256, upload.options.ContentSHA256)
		}

		var stdout bytes.Buffer
		_, err = ExportPartitionToWriterWithReadWorkers(
			newContext(),
			source,
			"snapshot",
			&stdout,
			1,
			1,
			2,
		)
		require.NoError(t, err)
		require.Equal(t, stdout.Bytes(), reconstructed.Bytes())
		require.Equal(
			t,
			make([]byte, chunkSize),
			reconstructed.Bytes()[chunkSize:2*chunkSize],
		)
		require.Equal(
			t,
			make([]byte, chunkSize),
			reconstructed.Bytes()[3*chunkSize:4*chunkSize],
		)
		last := dest.uploads[len(dest.uploads)-1]
		require.Equal(t, prefix+"manifest.json", last.key)
		require.Equal(t, "application/json", last.options.ContentType)
		require.True(t, last.options.ContentMD5)
		var stored S3Manifest
		require.NoError(t, json.Unmarshal(last.data, &stored))
		require.Equal(t, manifest, stored)
	}

	require.NotEqual(t, dest.uploads[0].key, dest.uploads[4].key)
	require.Equal(t, dest.uploads[3].key, dest.uploads[11].key)
	require.Equal(t, []string{"parent/manifest.json"}, dest.deleted)
	require.Len(t, dest.objects, 8)
}

func TestS3ExportOverwritesExistingBackup(t *testing.T) {
	for _, prefix := range []string{"", "/parent/"} {
		t.Run(
			prefix,
			func(t *testing.T) {
				source := &s3TestSource{
					meta: storage.SnapshotMeta{
						Size: chunkSize, ChunkCount: 1,
					},
				}

				dest := newS3TestDestination()
				options := s3TestOptions()
				options.Prefix = prefix
				options.Compression = "none"
				key := s3BackupPrefix(prefix) + "manifest.json"
				partKey := s3BackupPrefix(prefix) + "part-00000001.raw"
				unusedKey := s3BackupPrefix(prefix) + "part-00000002.raw"
				dest.objects[key] = []byte("existing backup")
				dest.objects[partKey] = []byte("old part")
				dest.objects[unusedKey] = []byte("unused old part")
				dest.beforePut = func(
					context.Context, string, persistence.S3Object,
				) error {

					dest.mu.Lock()
					defer dest.mu.Unlock()
					if _, exists := dest.objects[key]; exists {
						return errors.New("old completion marker still exists")
					}

					return nil
				}

				manifest, err := ExportToS3(
					newContext(), source, "snapshot", dest, options,
				)
				require.NoError(t, err)
				require.Equal(t, []string{key}, dest.deleted)
				require.Equal(t, make([]byte, chunkSize), dest.objects[partKey])
				require.Equal(
					t, []byte("unused old part"), dest.objects[unusedKey],
				)
				var stored S3Manifest
				require.NoError(t, json.Unmarshal(dest.objects[key], &stored))
				require.Equal(t, manifest, stored)
			},
		)
	}
}

func TestS3ExportManifestRemovalErrorDoesNotWriteParts(t *testing.T) {
	source := &s3TestSource{}
	dest := newS3TestDestination()
	options := s3TestOptions()
	key := s3BackupPrefix(options.Prefix) + "manifest.json"
	dest.objects[key] = []byte("existing backup")
	dest.deleteErr = errors.New("delete forbidden")
	_, err := ExportToS3(newContext(), source, "snapshot", dest, options)
	require.ErrorContains(t, err, "removing previous destination manifest")
	require.Zero(t, source.mapCalls.Load())
	require.Empty(t, dest.uploads)
	require.Equal(t, []byte("existing backup"), dest.objects[key])
}

func TestS3ExportFailedOverwriteLeavesNoManifest(t *testing.T) {
	source := &s3TestSource{
		meta: storage.SnapshotMeta{Size: chunkSize, ChunkCount: 1},
	}

	dest := newS3TestDestination()
	options := s3TestOptions()
	key := s3BackupPrefix(options.Prefix) + "manifest.json"
	dest.objects[key] = []byte("existing backup")
	dest.put = func(string, []byte) error {
		return errors.New("upload failed")
	}

	_, err := ExportToS3(newContext(), source, "snapshot", dest, options)
	require.ErrorContains(t, err, "upload failed")
	require.Equal(t, []string{key}, dest.deleted)
	_, exists := dest.objects[key]
	require.False(t, exists)
}

func TestS3ExportDestinationCheckErrorDoesNotWriteParts(t *testing.T) {
	source := &s3TestSource{}
	dest := newS3TestDestination()
	dest.existsErr = errors.New("destination unavailable")
	_, err := ExportToS3(
		newContext(), source, "snapshot", dest, s3TestOptions(),
	)
	require.ErrorContains(t, err, "checking destination manifest")
	require.Zero(t, source.mapCalls.Load())
	require.Empty(t, dest.uploads)
}

func TestS3ExportEmptyAndEncryptionMetadata(t *testing.T) {
	for _, mode := range []types.EncryptionMode{
		types.EncryptionMode_NO_ENCRYPTION,
		types.EncryptionMode_ENCRYPTION_AES_XTS,
		types.EncryptionMode_ENCRYPTION_WITH_ROOT_KMS_PROVIDED_KEY,
	} {
		t.Run(
			mode.String(),
			func(t *testing.T) {
				source := &s3TestSource{
					meta: storage.SnapshotMeta{
						Encryption: &types.EncryptionDesc{
							Mode: mode,
							Key: &types.EncryptionDesc_KeyHash{
								KeyHash: []byte("secret-key-material"),
							},
						},
					},
				}

				dest := newS3TestDestination()
				options := s3TestOptions()
				options.BlockSize = 512
				options.KekID = "disk-kek"
				options.EncryptedDEK = []byte{1, 2, 3}
				manifest, err := ExportToS3(
					newContext(),
					source,
					"snapshot",
					dest,
					options,
				)
				require.NoError(t, err)
				require.Empty(t, manifest.Parts)
				require.NotNil(t, manifest.Parts)
				require.Zero(t, source.mapCalls.Load())
				manifestUpload := dest.uploads[len(dest.uploads)-1]
				prefix := s3BackupPrefix(options.Prefix)
				require.Equal(t, prefix+"manifest.json", manifestUpload.key)
				require.Equal(t, mode.String(), manifest.EncryptionMode)
				if mode == types.EncryptionMode_NO_ENCRYPTION {
					require.Len(t, dest.uploads, 1)
					require.NotContains(t, dest.objects, prefix+"encrypted.dek")
					require.NotContains(t, dest.objects, prefix+"kek.id")
					require.Zero(t, manifest.BlockSize)
					require.NotContains(t, string(manifestUpload.data), `"block_size"`)
					require.Empty(t, manifest.KekID)
					require.Empty(t, manifest.EncryptedDEK)
					require.NotContains(t, string(manifestUpload.data), `"kek_id"`)
					require.NotContains(t, string(manifestUpload.data), `"encrypted_dek"`)
				} else {
					require.Len(t, dest.uploads, 3)
					require.Equal(t, prefix+"encrypted.dek", dest.uploads[0].key)
					require.Equal(t, []byte{1, 2, 3}, dest.objects[prefix+"encrypted.dek"])
					require.Equal(t, "application/octet-stream", dest.uploads[0].options.ContentType)
					require.True(t, dest.uploads[0].options.ContentMD5)
					require.Equal(t, prefix+"kek.id", dest.uploads[1].key)
					require.Equal(t, []byte("disk-kek"), dest.objects[prefix+"kek.id"])
					require.Equal(t, "text/plain", dest.uploads[1].options.ContentType)
					require.True(t, dest.uploads[1].options.ContentMD5)
					require.Equal(t, uint32(512), manifest.BlockSize)
					require.Contains(t, string(manifestUpload.data), `"block_size": 512`)
					require.Equal(t, "disk-kek", manifest.KekID)
					require.Equal(t, "AQID", manifest.EncryptedDEK)
					require.Contains(t, string(manifestUpload.data), `"kek_id": "disk-kek"`)
					require.Contains(t, string(manifestUpload.data), `"encrypted_dek": "AQID"`)
				}
				expected := "plaintext"
				if mode == types.EncryptionMode_ENCRYPTION_AES_XTS {
					expected = "ciphertext"
				}

				require.Equal(t, expected, manifest.RawDataInterpretation)
				require.NotContains(t, string(manifestUpload.data), "secret-key-material")
			},
		)
	}
}

func TestS3ExportPreservesBase64DecryptionInfo(t *testing.T) {
	source := &s3TestSource{meta: storage.SnapshotMeta{
		Encryption: &types.EncryptionDesc{
			Mode: types.EncryptionMode_ENCRYPTION_AES_XTS,
		},
	}}
	dest := newS3TestDestination()
	options := s3TestOptions()
	options.KekID = "disk-kek"
	options.DecryptionInfo = &S3DecryptionInfo{
		KekID:        "disk-kek",
		KekVersion:   "version-id",
		EncryptedDEK: "AQI=\n",
	}

	_, err := ExportToS3(newContext(), source, "snapshot", dest, options)
	require.NoError(t, err)

	prefix := s3BackupPrefix(options.Prefix)
	var manifest map[string]interface{}
	require.NoError(t, json.Unmarshal(
		dest.objects[prefix+"manifest.json"], &manifest,
	))
	require.Equal(t, "AQI=\n", manifest["encrypted_dek"])
	require.Equal(t, map[string]interface{}{
		"kek_id":        "disk-kek",
		"kek_version":   "version-id",
		"encrypted_dek": "AQI=\n",
	}, manifest["decryption_info"])
	require.Equal(t, []byte{1, 2}, dest.objects[prefix+"encrypted.dek"])
}

func TestS3ExportKeyFileFailuresDoNotPublishManifest(t *testing.T) {
	for _, filename := range []string{"encrypted.dek", "kek.id"} {
		for _, failure := range []string{"upload error", "canceled during upload"} {
			t.Run(filename+"/"+failure, func(t *testing.T) {
				ctx, cancel := context.WithCancel(newContext())
				defer cancel()
				source := &s3TestSource{meta: storage.SnapshotMeta{
					Size: chunkSize, ChunkCount: 1,
					Encryption: &types.EncryptionDesc{Mode: types.EncryptionMode_ENCRYPTION_AES_XTS},
				}}
				dest := newS3TestDestination()
				uploadErr := errors.New("key file upload failed")
				dest.put = func(key string, _ []byte) error {
					if strings.HasSuffix(key, "/"+filename) {
						if failure == "upload error" {
							return uploadErr
						}
						cancel()
					}
					return nil
				}
				options := s3TestOptions()
				options.KekID = "disk-kek"
				options.EncryptedDEK = []byte{1, 2, 3}
				manifest, err := ExportToS3(ctx, source, "snapshot", dest, options)
				if failure == "upload error" {
					require.ErrorIs(t, err, uploadErr)
					require.ErrorContains(t, err, "uploading "+filename)
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
				require.Len(t, manifest.Parts, 1)
				expectedUploads := 2
				if filename == "kek.id" {
					expectedUploads = 3
				}
				require.Len(t, dest.uploads, expectedUploads)
				for _, upload := range dest.uploads {
					require.False(t, strings.HasSuffix(upload.key, "/manifest.json"))
				}
			})
		}
	}
}

func TestS3ExportUnpackFiles(t *testing.T) {
	for _, prefix := range []string{"", "/parent/"} {
		for _, selected := range []string{"none", "archive", "readme", "both"} {
			t.Run(prefix+"/"+selected, func(t *testing.T) {
				options := s3TestOptions()
				options.Prefix = prefix
				archive := []byte{0x1f, 0x8b, 0, 0xff}
				readme := []byte("Инструкция\n")
				if selected == "archive" || selected == "both" {
					options.UnpackArchivePath = filepath.Join(t.TempDir(), "yc-nbs-export-unpack-0.1.0.tar.gz")
					require.NoError(t, os.WriteFile(options.UnpackArchivePath, archive, 0600))
				}
				if selected == "readme" || selected == "both" {
					options.UnpackReadmePath = filepath.Join(t.TempDir(), "instructions.txt")
					require.NoError(t, os.WriteFile(options.UnpackReadmePath, readme, 0600))
				}
				source := &s3TestSource{meta: storage.SnapshotMeta{
					Size: chunkSize, ChunkCount: 1,
					Encryption: &types.EncryptionDesc{Mode: types.EncryptionMode_ENCRYPTION_AES_XTS},
				}}
				options.KekID = "disk-kek"
				options.EncryptedDEK = []byte{1, 2, 3}
				dest := newS3TestDestination()
				if options.UnpackArchivePath != "" {
					// Later local changes must not affect the uploaded bytes or hash.
					dest.beforePut = func(_ context.Context, key string, _ persistence.S3Object) error {
						if strings.HasSuffix(key, "part-00000001.raw.lz4") {
							return os.WriteFile(options.UnpackArchivePath, []byte("changed"), 0600)
						}
						return nil
					}
				}
				_, err := ExportToS3(newContext(), source, "snapshot", dest, options)
				require.NoError(t, err)
				keyPrefix := s3BackupPrefix(prefix)
				expectedKeys := []string{
					keyPrefix + "part-00000001.raw.lz4",
					keyPrefix + "encrypted.dek",
					keyPrefix + "kek.id",
				}
				if options.UnpackArchivePath != "" {
					expectedKeys = append(expectedKeys, "yc-nbs-export-unpack-0.1.0.tar.gz")
					require.Equal(t, archive, dest.objects["yc-nbs-export-unpack-0.1.0.tar.gz"])
					expectedKeys = append(expectedKeys, "yc-nbs-export-unpack-0.1.0.tar.gz.sha256")
					require.Equal(t,
						"6adf81ba16d001cb9c84f1bcbe5bb40b4f6b2a11f1c0a79386b9ef1266add294  yc-nbs-export-unpack-0.1.0.tar.gz\n",
						string(dest.objects["yc-nbs-export-unpack-0.1.0.tar.gz.sha256"]),
					)
				}
				if options.UnpackReadmePath != "" {
					expectedKeys = append(expectedKeys, "README.md")
					require.Equal(t, readme, dest.objects["README.md"])
				}
				expectedKeys = append(expectedKeys, keyPrefix+"manifest.json")
				require.Len(t, dest.objects, len(expectedKeys))
				var actualKeys []string
				for _, upload := range dest.uploads {
					actualKeys = append(actualKeys, upload.key)
					if upload.key == "yc-nbs-export-unpack-0.1.0.tar.gz" {
						require.Equal(t, "application/gzip", upload.options.ContentType)
						require.True(t, upload.options.ContentMD5)
					}
					if upload.key == "README.md" {
						require.Equal(t, "text/markdown; charset=utf-8", upload.options.ContentType)
						require.True(t, upload.options.ContentMD5)
					}
					if upload.key == "yc-nbs-export-unpack-0.1.0.tar.gz.sha256" {
						require.Equal(t, "text/plain; charset=utf-8", upload.options.ContentType)
						require.True(t, upload.options.ContentMD5)
					}
				}
				require.Equal(t, expectedKeys, actualKeys)
			})
		}
	}
}

func TestS3ExportUnpackFilesSharedAcrossPrefixes(t *testing.T) {
	options := s3TestOptions()
	options.UnpackArchivePath = filepath.Join(t.TempDir(), "unpack.tar.gz")
	options.UnpackReadmePath = filepath.Join(t.TempDir(), "instructions.txt")
	require.NoError(t, os.WriteFile(options.UnpackArchivePath, []byte("archive"), 0600))
	require.NoError(t, os.WriteFile(options.UnpackReadmePath, []byte("readme"), 0600))
	source := &s3TestSource{meta: storage.SnapshotMeta{}}
	dest := newS3TestDestination()
	files := []string{"unpack.tar.gz", "unpack.tar.gz.sha256", "README.md"}
	for _, key := range files {
		dest.objects[key] = []byte("old")
	}
	dest.beforePut = func(_ context.Context, key string, _ persistence.S3Object) error {
		for _, file := range files {
			if key == file {
				require.Contains(t, dest.objects, key, "keep old object until PUT")
			}
		}
		return nil
	}
	for _, prefix := range []string{"first/backup", "second/backup"} {
		options.Prefix = prefix
		dest.checkedKeys = nil
		dest.deleted = nil
		dest.uploads = nil
		_, err := ExportToS3(newContext(), source, "snapshot", dest, options)
		require.NoError(t, err)
		require.Empty(t, dest.deleted)
		require.Zero(t, dest.readCalls)
		require.Equal(t, []string{prefix + "/manifest.json"}, dest.checkedKeys)
		require.Len(t, dest.uploads, len(files)+1)
		for i, key := range files {
			require.Equal(t, key, dest.uploads[i].key)
		}
		require.Equal(t, prefix+"/manifest.json", dest.uploads[len(files)].key)
		for _, key := range files {
			require.Contains(t, dest.objects, key)
			require.NotContains(t, dest.objects, prefix+"/"+key)
		}
	}
	require.Len(t, dest.objects, len(files)+2)
	require.Equal(t, []byte("archive"), dest.objects["unpack.tar.gz"])
	require.Equal(t, []byte("readme"), dest.objects["README.md"])
}

func TestS3ExportUnpackFileOverwriteFailurePreservesOldObject(t *testing.T) {
	options := s3TestOptions()
	options.UnpackReadmePath = filepath.Join(t.TempDir(), "README.md")
	require.NoError(t, os.WriteFile(options.UnpackReadmePath, []byte("new"), 0600))
	source := &s3TestSource{meta: storage.SnapshotMeta{}}
	dest := newS3TestDestination()
	dest.objects["README.md"] = []byte("old")
	uploadErr := errors.New("upload failed")
	dest.put = func(key string, _ []byte) error {
		if key == "README.md" {
			return uploadErr
		}
		return nil
	}
	_, err := ExportToS3(newContext(), source, "snapshot", dest, options)
	require.ErrorIs(t, err, uploadErr)
	require.ErrorContains(t, err, "uploading README.md")
	require.Equal(t, []byte("old"), dest.objects["README.md"])
	require.Empty(t, dest.deleted)
	require.Len(t, dest.uploads, 1)
	require.Equal(t, "README.md", dest.uploads[0].key)
	require.NotContains(t, dest.objects, s3BackupPrefix(options.Prefix)+"manifest.json")
}

func TestS3ExportUnpackFileFailures(t *testing.T) {
	for _, filename := range []string{"yc-nbs-export-unpack-0.1.0.tar.gz", "yc-nbs-export-unpack-0.1.0.tar.gz.sha256", "README.md"} {
		for _, failure := range []string{"missing", "directory", "upload", "cancel"} {
			if filename == "yc-nbs-export-unpack-0.1.0.tar.gz.sha256" && (failure == "missing" || failure == "directory") {
				// The checksum is computed; no local checksum file is read.
				continue
			}
			t.Run(filename+"/"+failure, func(t *testing.T) {
				ctx, cancel := context.WithCancel(newContext())
				defer cancel()
				path := filepath.Join(t.TempDir(), strings.TrimSuffix(filename, ".sha256"))
				if failure == "directory" {
					require.NoError(t, os.Mkdir(path, 0700))
				} else if failure != "missing" {
					require.NoError(t, os.WriteFile(path, []byte("file"), 0600))
				}
				options := s3TestOptions()
				if filename == "README.md" {
					options.UnpackReadmePath = path
				} else {
					options.UnpackArchivePath = path
				}
				source := &s3TestSource{meta: storage.SnapshotMeta{Size: chunkSize, ChunkCount: 1}}
				dest := newS3TestDestination()
				manifestKey := s3BackupPrefix(options.Prefix) + "manifest.json"
				dest.objects[manifestKey] = []byte("old manifest")
				uploadErr := errors.New("upload failed")
				dest.put = func(key string, _ []byte) error {
					if key == filename {
						if failure == "cancel" {
							cancel()
							return ctx.Err()
						}
						return uploadErr
					}
					return nil
				}
				_, err := ExportToS3(ctx, source, "snapshot", dest, options)
				if failure == "missing" || failure == "directory" {
					require.ErrorContains(t, err, "reading export file "+filename)
					require.Equal(t, []byte("old manifest"), dest.objects[manifestKey])
					require.Empty(t, dest.deleted)
					require.Empty(t, dest.uploads)
					require.Zero(t, source.readCalls.Load())
				} else {
					if failure == "cancel" {
						require.ErrorIs(t, err, context.Canceled)
					} else {
						require.ErrorIs(t, err, uploadErr)
					}
					require.ErrorContains(t, err, "uploading "+filename)
					require.NotContains(t, dest.objects, manifestKey)
					expectedUploads := 2
					if filename == "yc-nbs-export-unpack-0.1.0.tar.gz.sha256" {
						expectedUploads = 3
						require.Contains(t, dest.objects, "yc-nbs-export-unpack-0.1.0.tar.gz")
					}
					require.Len(t, dest.uploads, expectedUploads)
				}
			})
		}
	}
}

func TestS3ExportOptionalKeyFiles(t *testing.T) {
	for _, tc := range []struct {
		name         string
		kekID        string
		encryptedDEK []byte
	}{
		{name: "no keys"},
		{name: "KEK ID only", kekID: "disk-kek"},
		{name: "encrypted DEK only", encryptedDEK: []byte{1, 2, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &s3TestSource{meta: storage.SnapshotMeta{
				Encryption: &types.EncryptionDesc{Mode: types.EncryptionMode_ENCRYPTION_AES_XTS},
			}}
			dest := newS3TestDestination()
			options := s3TestOptions()
			options.KekID = tc.kekID
			options.EncryptedDEK = tc.encryptedDEK
			_, err := ExportToS3(newContext(), source, "snapshot", dest, options)
			require.NoError(t, err)
			prefix := s3BackupPrefix(options.Prefix)
			if tc.kekID == "" {
				require.NotContains(t, dest.objects, prefix+"kek.id")
			} else {
				require.Equal(t, []byte(tc.kekID), dest.objects[prefix+"kek.id"])
			}
			if len(tc.encryptedDEK) == 0 {
				require.NotContains(t, dest.objects, prefix+"encrypted.dek")
			} else {
				require.Equal(t, tc.encryptedDEK, dest.objects[prefix+"encrypted.dek"])
			}
			require.True(t, strings.HasSuffix(dest.uploads[len(dest.uploads)-1].key, "/manifest.json"))
		})
	}
}

func TestS3ExportEarlyFailuresDoNotReadPayload(t *testing.T) {
	for _, tc := range []string{
		"not-ready",
		"metadata",
		"missing-bucket",
		"bucket-permission",
		"workers",
		"upload-workers",
		"part-size",
		"compression",
		"canceled",
	} {
		t.Run(
			tc,
			func(t *testing.T) {
				source := &s3TestSource{
					meta: storage.SnapshotMeta{
						Size:       chunkSize,
						ChunkCount: 1,
					},
				}

				dest := newS3TestDestination()
				options := s3TestOptions()
				ctx, cancel := context.WithCancel(newContext())
				defer cancel()
				switch tc {
				case "not-ready":
					source.readyErr = errors.New("not ready")
				case "metadata":
					source.meta.ChunkCount = 0
				case "missing-bucket":
					dest.exists = false
				case "bucket-permission":
					dest.bucketErr = errors.New("access denied")
				case "workers":
					options.ReadWorkerCount = 0
				case "upload-workers":
					options.UploadWorkerCount = 0
				case "part-size":
					options.PartSize = 1
				case "compression":
					options.Compression = "gzip"
				case "canceled":
					cancel()
				}

				_, err := ExportToS3(ctx, source, "snapshot", dest, options)
				require.Error(t, err)
				require.Zero(t, source.mapCalls.Load())
				require.Zero(t, source.readCalls.Load())
				require.Empty(t, dest.uploads)
			},
		)
	}
}

func TestS3ExportFailuresPreventManifest(t *testing.T) {
	for _, tc := range []string{"read", "map", "upload", "cancel-after-part"} {
		t.Run(
			tc,
			func(t *testing.T) {
				source := &s3TestSource{
					meta: storage.SnapshotMeta{
						Size:       2 * chunkSize,
						ChunkCount: 2,
					},
					entries: []storage.ChunkMapEntry{{
						ChunkIndex: 0,
						ChunkID:    "data",
					}},
				}

				dest := newS3TestDestination()
				options := s3TestOptions()
				options.PartSize = chunkSize
				ctx, cancel := context.WithCancel(newContext())
				defer cancel()
				switch tc {
				case "read":
					source.readErr = errors.New("source disappeared")
				case "map":
					source.mapErr = errors.New("map failed")
				case "upload":
					dest.put = func(key string, data []byte) error {
						if strings.HasSuffix(key, "part-00000002.raw.lz4") {
							return task_errors.NewNonRetriableErrorf("access denied")
						}

						return nil
					}

				case "cancel-after-part":
					dest.put = func(string, []byte) error {
						cancel()
						return nil
					}
				}

				_, err := ExportToS3(ctx, source, "snapshot", dest, options)
				require.Error(t, err)
				require.Contains(t, err.Error(), "s3://destination/parent/")
				for key := range dest.objects {
					require.False(t, strings.HasSuffix(key, "manifest.json"))
				}

				require.Zero(t, source.activeReads.Load())
				require.Zero(t, dest.readCalls)
			},
		)
	}
}

func TestS3ExportAmbiguousManifestPublication(t *testing.T) {
	for _, tc := range []string{
		"confirmed",
		"unavailable",
		"different",
		"permanent",
	} {
		t.Run(
			tc,
			func(t *testing.T) {
				dest := newS3TestDestination()
				dest.put = func(key string, data []byte) error {
					if tc == "permanent" {
						return task_errors.NewNonRetriableErrorf("denied")
					}

					dest.objects[key] = data // Remote success, response lost.
					return task_errors.NewRetriableErrorf("connection reset")
				}

				if tc == "unavailable" {
					dest.readErr = errors.New("read denied")
				}

				dest.readMismatch = tc == "different"
				_, err := ExportToS3(
					newContext(),
					&s3TestSource{},
					"snapshot",
					dest,
					s3TestOptions(),
				)
				if tc == "confirmed" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), "publication is uncertain")
				}

				require.Equal(t, 1, dest.readCalls)
			},
		)
	}
}

func TestS3ExportCancellationWaitsForReadWorkers(t *testing.T) {
	source := &s3TestSource{
		meta: storage.SnapshotMeta{
			Size:       2 * chunkSize,
			ChunkCount: 2,
		},
		entries: []storage.ChunkMapEntry{
			{
				ChunkIndex: 0,
				ChunkID:    "a",
			},
			{
				ChunkIndex: 1,
				ChunkID:    "b",
			},
		},
		blockReads:  true,
		readStarted: make(chan struct{}, 2),
	}

	dest := newS3TestDestination()
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := ExportToS3(
			ctx,
			source,
			"snapshot",
			dest,
			s3TestOptions(),
		)
		done <- err
	}()

	select {
	case <-source.readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("read did not start")
	}

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("export did not stop")
	}

	require.Zero(t, source.activeReads.Load())
	require.Empty(t, dest.uploads)
}

func TestS3CoverageValidation(t *testing.T) {
	sum := sha256.Sum256([]byte("a"))
	valid := S3Manifest{
		FormatVersion: 1,
		Format:        "partitioned-raw-image",
		Compression:   "none",
		Size:          1,
		MaxPartSize:   chunkSize,
		PartCount:     1,
		Parts: []S3ManifestPart{
			{
				Number:       1,
				Key:          "part-00000001.raw",
				Length:       1,
				StoredLength: 1,
				SHA256:       hex.EncodeToString(sum[:]),
			},
		},
	}

	require.NoError(t, validateS3Manifest(valid))
	for _, tc := range []string{
		"gap",
		"length",
		"count",
		"checksum",
		"overflow",
		"overlap",
	} {
		manifest := valid
		manifest.Parts = append([]S3ManifestPart(nil), valid.Parts...)
		switch tc {
		case "gap":
			manifest.Parts[0].Offset = 1
		case "length":
			manifest.Size = 2
		case "count":
			manifest.PartCount = 2
		case "checksum":
			manifest.Parts[0].SHA256 = "wrong"
		case "overflow":
			manifest.Parts[0].Length = math.MaxUint64
		case "overlap":
			manifest.Parts = append(manifest.Parts, manifest.Parts[0])
			manifest.PartCount = 2
		}

		require.Error(t, validateS3Manifest(manifest), tc)
	}
}

func TestS3ReadPartUsesDestinationSlicesAndClearsReusedZeroes(t *testing.T) {
	meta := storage.SnapshotMeta{
		Size:       3*chunkSize + 321,
		ChunkCount: 4,
	}

	source := &s3TestSource{
		meta: meta,
		entries: []storage.ChunkMapEntry{
			{
				ChunkIndex: 0,
				ChunkID:    "data-0",
				StoredInS3: true,
			},
			{ChunkIndex: 1}, // Explicit zeroes in the first part.
			// Chunk 2 is absent; the second part reuses chunk 0's buffer.
			{
				ChunkIndex: 3,
				ChunkID:    "data-3",
			},
		},
	}

	buffer := &s3PartBuffer{data: bytes.Repeat([]byte{0xa5}, 2*chunkSize)}
	source.readHook = func(ctx context.Context, chunk *common.Chunk) error {
		if len(chunk.Data) != chunkSize {
			return fmt.Errorf("checksum requires a complete 4MiB chunk")
		}

		if chunk.Index == 0 && &chunk.Data[0] != &buffer.data[0] {
			return fmt.Errorf("full chunk must be decoded directly into the part")
		}

		if chunk.Index == 3 && &chunk.Data[0] == &buffer.data[chunkSize] {
			return fmt.Errorf("partial chunk needs a complete checksum buffer")
		}

		for i := range chunk.Data {
			chunk.Data[i] = chunkDataByte(chunk.Index)
		}

		return nil
	}

	manifest := &S3Manifest{
		SnapshotID: "snapshot",
		PartCount:  2,
	}

	progress := &s3ExportProgress{}
	expected := makeExpectedPartitionSnapshot(meta.Size, source.entries)
	for number := uint32(1); number <= 2; number++ {
		part, err := readS3Part(
			newContext(),
			source,
			meta,
			manifest,
			s3TestOptions(),
			number,
			buffer,
			progress,
		)
		require.NoError(t, err)
		start := int(part.part.Offset)
		end := start + int(part.part.Length)
		require.Equal(t, expected[start:end], part.buffer.data)
		require.Same(t, buffer, part.buffer)
	}

	require.EqualValues(t, 2, source.readCalls.Load())
	require.Equal(t, meta.Size, progress.state.read)
}

func TestS3ReadFailureWaitsForOtherReadersBeforeReturningBuffer(t *testing.T) {
	source := &s3TestSource{
		meta: storage.SnapshotMeta{
			Size:       2 * chunkSize,
			ChunkCount: 2,
		},
		entries: []storage.ChunkMapEntry{
			{
				ChunkIndex: 0,
				ChunkID:    "fail",
			},
			{
				ChunkIndex: 1,
				ChunkID:    "wait",
			},
		},
	}

	ctx, cancel := context.WithTimeout(newContext(), 5*time.Second)
	defer cancel()
	secondStarted := make(chan struct{})
	readErr := errors.New("chunk read failed")
	source.readHook = func(ctx context.Context, chunk *common.Chunk) error {
		if chunk.Index == 0 {
			select {
			case <-secondStarted:
				return readErr
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		close(secondStarted)
		<-ctx.Done()
		// Even writes performed while handling cancellation must finish
		// before the caller can reuse or upload the part buffer.
		chunk.Data[0] = 0xab
		return ctx.Err()
	}

	buffer := &s3PartBuffer{}
	_, err := readS3Part(
		ctx,
		source,
		source.meta,
		&S3Manifest{
			SnapshotID: "snapshot",
			PartCount:  1,
		},
		s3TestOptions(),
		1,
		buffer,
		&s3ExportProgress{},
	)
	require.ErrorIs(t, err, readErr)
	require.Zero(t, source.activeReads.Load())
	require.Equal(t, byte(0xab), buffer.data[chunkSize])
}

func TestS3ExportOverlapsUploadsAndBoundsReadAhead(t *testing.T) {
	source := &s3TestSource{
		meta: storage.SnapshotMeta{
			Size:       5 * chunkSize,
			ChunkCount: 5,
		},
	}

	for i := uint32(0); i < 5; i++ {
		source.entries = append(
			source.entries,
			storage.ChunkMapEntry{
				ChunkIndex: i,
				ChunkID:    fmt.Sprintf("data-%v", i),
			},
		)
	}

	dest := newS3TestDestination()
	started := make(chan struct{}, 2)
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	thirdStarted := make(chan struct{})
	var uploadStarts atomic.Int32
	dest.beforePut = func(
		ctx context.Context,
		key string,
		object persistence.S3Object,
	) error {

		if strings.HasSuffix(key, "manifest.json") {
			return nil
		}

		sum := sha256.Sum256(object.Data)
		var release <-chan struct{}
		switch uploadStarts.Add(1) {
		case 1:
			release = releaseFirst
		case 2:
			release = releaseSecond
		case 3:
			close(thirdStarted)
		}

		if release != nil {
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if sum != sha256.Sum256(object.Data) {
			return fmt.Errorf("part buffer changed during upload")
		}

		return nil
	}

	options := s3TestOptions()
	options.PartSize = chunkSize
	options.UploadWorkerCount = 2
	ctx, cancel := context.WithTimeout(newContext(), 5*time.Second)
	defer cancel()
	type result struct {
		manifest S3Manifest
		err      error
	}

	done := make(chan result, 1)
	go func() {
		manifest, err := ExportToS3(ctx, source, "snapshot", dest, options)
		done <- result{manifest, err}
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("two concurrent uploads did not start")
		}
	}

	require.Eventually(
		t,
		func() bool { return source.readCalls.Load() >= 3 },
		time.Second,
		time.Millisecond,
	)
	// Two blocked uploads leave one shared buffer for assembly.
	require.EqualValues(t, 3, source.readCalls.Load())
	close(releaseSecond)
	select {
	case <-thirdStarted:
	case <-ctx.Done():
		t.Fatal(
			"upload did not advance while the first part was still blocked",
		)
	}

	close(releaseFirst)

	var exported result
	select {
	case exported = <-done:
	case <-ctx.Done():
		t.Fatal("export did not finish")
	}

	require.NoError(t, exported.err)
	require.NoError(t, validateS3Manifest(exported.manifest))
	prefix := s3BackupPrefix(options.Prefix)
	for i, part := range exported.manifest.Parts {
		require.Equal(t, uint32(i+1), part.Number)
		data := dest.objects[prefix+part.Key]
		sum := sha256.Sum256(data)
		require.Equal(t, part.SHA256, hex.EncodeToString(sum[:]))
		decoded := decodeS3TestPart(t, exported.manifest.Compression, part, data)
		require.Equal(t, chunkDataByte(uint32(i)), decoded[0])
	}

	require.Equal(
		t,
		prefix+"manifest.json",
		dest.uploads[len(dest.uploads)-1].key,
	)
	require.Zero(t, source.activeReads.Load())
}

func TestS3ExportUploadFailureCancelsOtherWorkers(t *testing.T) {
	source := &s3TestSource{
		meta: storage.SnapshotMeta{
			Size:       5 * chunkSize,
			ChunkCount: 5,
		},
	}

	dest := newS3TestDestination()
	started := make(chan struct{}, 2)
	fail := make(chan struct{})
	uploadErr := errors.New("upload failed")
	var active atomic.Int32
	var uploadStarts atomic.Int32
	dest.beforePut = func(
		ctx context.Context,
		key string,
		object persistence.S3Object,
	) error {

		if strings.HasSuffix(key, "manifest.json") {
			return fmt.Errorf("manifest must not be uploaded")
		}

		active.Add(1)
		defer active.Add(-1)
		started <- struct{}{}
		if uploadStarts.Add(1) == 1 {
			select {
			case <-fail:
				return uploadErr
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		<-ctx.Done()
		return ctx.Err()
	}

	options := s3TestOptions()
	options.PartSize = chunkSize
	options.UploadWorkerCount = 2
	ctx, cancel := context.WithTimeout(newContext(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := ExportToS3(ctx, source, "snapshot", dest, options)
		done <- err
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("two concurrent uploads did not start")
		}
	}

	close(fail)
	select {
	case err := <-done:
		require.ErrorIs(t, err, uploadErr)
	case <-ctx.Done():
		t.Fatal("upload failure did not stop the export")
	}

	require.Zero(t, active.Load())
	require.Zero(t, source.activeReads.Load())
	require.Empty(t, dest.objects)
}

func TestS3ExportSlowChunkDoesNotBlockOtherParts(t *testing.T) {
	source := &s3TestSource{
		meta: storage.SnapshotMeta{
			Size:       6 * chunkSize,
			ChunkCount: 6,
		},
	}

	for i := uint32(0); i < 6; i++ {
		source.entries = append(
			source.entries,
			storage.ChunkMapEntry{
				ChunkIndex: i,
				ChunkID:    fmt.Sprintf("data-%v", i),
			},
		)
	}

	ctx, cancel := context.WithTimeout(newContext(), 5*time.Second)
	defer cancel()
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	source.readHook = func(ctx context.Context, chunk *common.Chunk) error {
		if source.activeReads.Load() > 2 {
			return fmt.Errorf("shared read worker limit exceeded")
		}

		if chunk.Index == 0 {
			close(slowStarted)
			select {
			case <-releaseSlow:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		for i := range chunk.Data {
			chunk.Data[i] = chunkDataByte(chunk.Index)
		}

		return nil
	}

	dest := newS3TestDestination()
	fastUploaded := make(chan struct{})
	var signaled atomic.Bool
	dest.put = func(key string, data []byte) error {
		if strings.HasSuffix(key, "manifest.json") {
			return nil
		}

		if !strings.HasSuffix(key, "part-00000001.raw.lz4") &&
			signaled.CompareAndSwap(false, true) {

			close(fastUploaded)
		}

		return nil
	}

	options := s3TestOptions()
	options.UploadWorkerCount = 2
	done := make(chan error, 1)
	go func() {
		manifest, err := ExportToS3(ctx, source, "snapshot", dest, options)
		if err == nil {
			err = validateS3Manifest(manifest)
		}

		done <- err
	}()

	select {
	case <-slowStarted:
	case <-ctx.Done():
		t.Fatal("slow chunk did not start")
	}

	select {
	case <-fastUploaded:
	case <-ctx.Done():
		t.Fatal("slow first part blocked uploads of the other parts")
	}

	close(releaseSlow)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("export did not finish")
	}

	require.EqualValues(t, 6, source.readCalls.Load())
	require.Zero(t, source.activeReads.Load())
	require.Len(t, dest.objects, 4)
}

func TestS3ExportCancellationWaitsForConcurrentAssemblers(t *testing.T) {
	source := &s3TestSource{
		meta: storage.SnapshotMeta{
			Size:       6 * chunkSize,
			ChunkCount: 6,
		},
		blockReads:  true,
		readStarted: make(chan struct{}, 2),
	}

	for i := uint32(0); i < 6; i++ {
		source.entries = append(
			source.entries,
			storage.ChunkMapEntry{
				ChunkIndex: i,
				ChunkID:    fmt.Sprintf("data-%v", i),
			},
		)
	}

	ctx, cancel := context.WithTimeout(newContext(), 5*time.Second)
	defer cancel()
	dest := newS3TestDestination()
	options := s3TestOptions()
	options.UploadWorkerCount = 2
	done := make(chan error, 1)
	go func() {
		_, err := ExportToS3(ctx, source, "snapshot", dest, options)
		done <- err
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-source.readStarted:
		case <-ctx.Done():
			t.Fatal("shared readers did not start")
		}
	}

	require.Eventually(
		t,
		func() bool { return source.mapCalls.Load() >= 2 },
		time.Second,
		time.Millisecond,
	)
	require.EqualValues(t, 2, source.readCalls.Load())
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("assemblers did not stop")
	}

	require.Zero(t, source.activeReads.Load())
	require.Empty(t, dest.uploads)
}

func TestS3SharedReadSourcePreservesRangeCapability(t *testing.T) {
	source := &partitionRangeStorage{
		entries: []storage.ChunkMapEntry{{ChunkIndex: 2}},
	}

	reader := newS3ReadSource(source, 2, &s3ExportProgress{}, 0)
	rangeReader, ok := reader.(storage.ChunkMapRangeReader)
	require.True(t, ok)
	entries, errors := rangeReader.ReadChunkMapRange(
		newContext(),
		"snapshot",
		2,
		4,
	)
	var actual []storage.ChunkMapEntry
	for entry := range entries {
		actual = append(actual, entry)
	}

	require.NoError(t, <-errors)
	require.Equal(t, source.entries, actual)
	require.EqualValues(t, 2, source.start)
	require.EqualValues(t, 4, source.end)
	unbounded := newS3ReadSource(&s3TestSource{}, 2, &s3ExportProgress{}, 0)
	_, ok = unbounded.(storage.ChunkMapRangeReader)
	require.False(t, ok)
}
