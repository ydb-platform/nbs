package exporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofrs/uuid"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

const DefaultS3PartSize = 256 * 1024 * 1024
const MaxS3PartSize = 1024 * 1024 * 1024
const DefaultS3UploadWorkerCount = 2
const MaxS3UploadWorkerCount = 64
const DefaultS3Compression = "lz4"
const MaxS3ManifestSize = 256 * 1024 * 1024

// S3Destination is the narrow destination API needed by raw export.
// PutObjectWithOptions must support concurrent calls with independent objects.
type S3Destination interface {
	BucketExists(context.Context, string) (bool, error)
	ObjectExists(context.Context, string, string) (bool, error)
	DeleteObject(context.Context, string, string) error
	PutObjectWithOptions(
		context.Context,
		string,
		string,
		persistence.S3Object,
		persistence.S3PutObjectOptions,
	) error
	GetObjectWithLimit(context.Context, string, string, int64) ([]byte, error)
}

type S3ExportOptions struct {
	// Optional registry for aggregate transfer metrics; never persisted.
	MetricsRegistry metrics.Registry
	// MaxRetries limits retries per source/destination operation after its
	// initial attempt. Zero disables retries; -1 retries until ctx is canceled.
	// Destination calls retain their individual timeouts.
	MaxRetries int
	// ProgressFile enables durable checkpoints and automatic resume.
	ProgressFile string
	// Identities bind a checkpoint to the caller's source and endpoint.
	SourceIdentity    string
	Endpoint          string
	Bucket            string
	Prefix            string
	Compression       string
	PartSize          uint64
	ReadWorkerCount   int
	UploadWorkerCount int
	BlockSize         uint32
	KekID             string
	// Raw encrypted DEK from synchronous exports without DecryptionInfo.
	EncryptedDEK []byte
	// Opaque metadata supplied by Compute, copied without decrypting data.
	DecryptionInfo    *S3DecryptionInfo
	UnpackArchivePath string
	UnpackReadmePath  string
}

type S3DecryptionInfo struct {
	KekID      string `json:"kek_id"`
	KekVersion string `json:"kek_version"`
	// Base64 supplied by Compute, preserved without re-encoding.
	EncryptedDEK string `json:"encrypted_dek"`
}

// S3ExportState is checkpointed before any parts are written and after each
// successful upload. The caller keeps exclusive ownership of the destination
// prefix and keeps the source immutable until completion or cancellation.
type S3ExportState struct {
	Bucket              string     `json:"bucket"`
	Prefix              string     `json:"prefix"`
	Manifest            S3Manifest `json:"manifest"`
	Initialized         bool       `json:"initialized"`
	DestinationPrepared bool       `json:"destination_prepared"`
	Completed           bool       `json:"completed"`
}

type s3ExportFile struct {
	key         string
	contentType string
	data        []byte
}

func readS3ExportFiles(options S3ExportOptions) ([]s3ExportFile, error) {
	var files []s3ExportFile
	for _, file := range []struct {
		path        string
		key         string
		contentType string
		checksum    bool
	}{
		{options.UnpackArchivePath, filepath.Base(options.UnpackArchivePath), "application/gzip", true},
		{options.UnpackReadmePath, "README.md", "text/markdown; charset=utf-8", false},
	} {
		if file.path == "" {
			continue
		}

		data, err := os.ReadFile(file.path)
		if err != nil {
			return nil, fmt.Errorf("reading export file %v: %w", file.key, err)
		}
		files = append(files, s3ExportFile{file.key, file.contentType, data})
		if file.checksum {
			// Hash the same bytes that will be uploaded, before exporting them.
			sum := sha256.Sum256(data)
			files = append(files, s3ExportFile{
				key:         file.key + ".sha256",
				contentType: "text/plain; charset=utf-8",
				data:        []byte(fmt.Sprintf("%x  %s\n", sum, file.key)),
			})
		}
	}
	return files, nil
}

// S3Manifest is the completion marker. Parts are reconstructed in this order,
// verifying each length and SHA-256; object listing order and ETags are
// irrelevant. SnapshotID is the dataplane snapshot ID, also for images.
type S3Manifest struct {
	FormatVersion  uint32    `json:"format_version"`
	Format         string    `json:"format"`
	Compression    string    `json:"compression"`
	SnapshotID     string    `json:"snapshot_id"`
	BackupUUID     string    `json:"backup_uuid"`
	CreatedAt      time.Time `json:"created_at"`
	Size           uint64    `json:"size"`
	EncryptionMode string    `json:"encryption_mode"`
	// Logical disk block size in bytes, when recorded by snapshot creation.
	BlockSize uint32 `json:"block_size,omitempty"`
	// Optional base64-encoded disk key hash, never secret key material.
	EncryptionKeyHash []byte `json:"encryption_key_hash,omitempty"`
	// KMS key ID and base64-encoded encrypted data key from the source disk.
	KekID          string            `json:"kek_id,omitempty"`
	EncryptedDEK   string            `json:"encrypted_dek,omitempty"`
	DecryptionInfo *S3DecryptionInfo `json:"decryption_info,omitempty"`

	RawDataInterpretation string           `json:"raw_data_interpretation"`
	MaxPartSize           uint64           `json:"max_part_size"`
	PartCount             uint32           `json:"part_count"`
	Parts                 []S3ManifestPart `json:"parts"`
}

type S3ManifestPart struct {
	Number       uint32 `json:"number"`
	Key          string `json:"key"`
	Offset       uint64 `json:"offset"`
	Length       uint64 `json:"length"`
	StoredLength uint64 `json:"stored_length"`
	SHA256       string `json:"sha256"`
}

func ValidateS3Compression(compression string) error {
	if compression != "lz4" && compression != "none" {
		return fmt.Errorf(
			"S3 compression must be lz4 or none, got %q",
			compression,
		)
	}

	return nil
}

func s3PartKey(number uint32, compression string) string {
	key := fmt.Sprintf("part-%08d.raw", number)
	if compression == "lz4" {
		key += ".lz4"
	}

	return key
}

func ValidateReadWorkers(count int) error {
	if count <= 0 {
		return task_errors.NewNonRetriableErrorf(
			"readWorkerCount must be positive, got %v",
			count,
		)
	}

	if count > math.MaxInt/streamReadAheadMultiplier {
		return task_errors.NewNonRetriableErrorf(
			"invalid read worker count %v",
			count,
		)
	}

	return nil
}

func ValidateS3PartSize(size uint64) error {
	if size == 0 || size%chunkSize != 0 || size > MaxS3PartSize {
		return task_errors.NewNonRetriableErrorf(
			"S3 part size must be a positive multiple of 4 MiB, "+
				"at most 1 GiB; got %v bytes",
			size,
		)
	}

	return nil
}

func ValidateS3UploadWorkers(count int) error {
	if count <= 0 || count > MaxS3UploadWorkerCount {
		return task_errors.NewNonRetriableErrorf(
			"S3 upload worker count must be in [1, %v], got %v",
			MaxS3UploadWorkerCount,
			count,
		)
	}

	return nil
}

func s3PartitionCount(
	meta storage.SnapshotMeta,
	partSize uint64,
) (uint32, error) {

	if err := ValidateS3PartSize(partSize); err != nil {
		return 0, err
	}

	// Division with remainder avoids overflowing Size + chunkSize - 1.
	chunks := meta.Size / chunkSize
	if meta.Size%chunkSize != 0 {
		chunks++
	}

	if chunks > math.MaxUint32 || chunks != uint64(meta.ChunkCount) {
		return 0, task_errors.NewNonRetriableErrorf(
			"inconsistent snapshot metadata: size %v, chunk count %v",
			meta.Size,
			meta.ChunkCount,
		)
	}

	chunksPerPart := partSize / chunkSize
	count := chunks / chunksPerPart
	if chunks%chunksPerPart != 0 {
		count++
	}

	return uint32(count), nil
}

func s3BackupPrefix(parent string) string {
	prefix := strings.Trim(parent, "/")
	if prefix != "" {
		prefix += "/"
	}

	return prefix
}

func validateS3Manifest(manifest S3Manifest) error {
	if manifest.FormatVersion != 1 || manifest.Format != "partitioned-raw-image" {
		return fmt.Errorf("unsupported S3 manifest format or version")
	}

	if err := ValidateS3Compression(manifest.Compression); err != nil {
		return err
	}

	if err := ValidateS3PartSize(manifest.MaxPartSize); err != nil {
		return err
	}

	if uint64(len(manifest.Parts)) != uint64(manifest.PartCount) {
		return fmt.Errorf("manifest part count mismatch")
	}

	var offset uint64
	for i, part := range manifest.Parts {
		if part.Number != uint32(i+1) || part.Offset != offset {
			return fmt.Errorf("invalid manifest part %v", i+1)
		}
		if err := validateS3ManifestPart(manifest, part); err != nil {
			return err
		}

		offset += part.Length
	}

	if offset != manifest.Size {
		return fmt.Errorf(
			"manifest covers %v bytes, expected %v",
			offset,
			manifest.Size,
		)
	}

	return nil
}

func validateS3ManifestPart(manifest S3Manifest, part S3ManifestPart) error {
	sum, err := hex.DecodeString(part.SHA256)
	if part.Key != s3PartKey(part.Number, manifest.Compression) ||
		part.Length == 0 || part.Length > manifest.MaxPartSize ||
		part.Offset > manifest.Size || part.Length > manifest.Size-part.Offset ||
		part.StoredLength == 0 ||
		(manifest.Compression == "none" && part.StoredLength != part.Length) ||
		(manifest.Compression == "lz4" && part.StoredLength > part.Length+s3FrameOverhead) ||
		part.SHA256 != strings.ToLower(part.SHA256) || err != nil || len(sum) != sha256.Size {
		return fmt.Errorf("invalid manifest part %v", part.Number)
	}
	return nil
}

func marshalS3Manifest(manifest S3Manifest, limit int) ([]byte, error) {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}

	if len(data) > limit {
		return nil, fmt.Errorf("serialized S3 manifest exceeds %v bytes", limit)
	}

	return data, nil
}

func copySpeedMiBPerSecond(size uint64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}

	return float64(size) / (1024 * 1024) / elapsed.Seconds()
}

// ExportToS3 synchronously exports a complete ready snapshot, publishing its
// completion manifest only after every raw part and companion file succeeds.
// Source access is read-only; the caller must keep it available during export.
func ExportToS3(
	ctx context.Context,
	snapshotStorage Source,
	snapshotID string,
	destination S3Destination,
	options S3ExportOptions,
) (S3Manifest, error) {

	return ExportToS3Resumable(
		ctx, snapshotStorage, snapshotID, destination, options,
		&S3ExportState{}, func(context.Context) error { return nil },
	)
}

// ExportToS3Resumable resumes parts confirmed by a successful PutObject and a
// durable saveState. An ambiguous or uncheckpointed PUT repeats only that part.
func ExportToS3Resumable(
	ctx context.Context,
	snapshotStorage Source,
	snapshotID string,
	destination S3Destination,
	options S3ExportOptions,
	state *S3ExportState,
	saveState func(context.Context) error,
) (S3Manifest, error) {

	if state.Initialized && (state.Bucket != options.Bucket ||
		state.Prefix != s3BackupPrefix(options.Prefix)) {

		return state.Manifest, task_errors.NewNonRetriableErrorf(
			"destination changed since export checkpoint",
		)
	}

	if options.Compression == "" {
		options.Compression = DefaultS3Compression
	}

	if err := ValidateS3Compression(options.Compression); err != nil {
		return S3Manifest{}, err
	}

	if snapshotID == "" || options.Bucket == "" {
		return S3Manifest{}, fmt.Errorf(
			"snapshot ID and destination bucket are required",
		)
	}

	if err := ValidateReadWorkers(options.ReadWorkerCount); err != nil {
		return S3Manifest{}, err
	}

	if err := ValidateS3UploadWorkers(options.UploadWorkerCount); err != nil {
		return S3Manifest{}, err
	}

	if err := ValidateS3PartSize(options.PartSize); err != nil {
		return S3Manifest{}, err
	}
	if options.MaxRetries < -1 {
		return S3Manifest{}, task_errors.NewNonRetriableErrorf("S3 max retries must be -1 or nonnegative")
	}

	if err := ctx.Err(); err != nil {
		return S3Manifest{}, err
	}

	transferMetrics := newS3ExportMetrics(options.MetricsRegistry)
	ctx = context.WithValue(ctx, s3ExportMetricsContextKey{}, transferMetrics)

	// Read configured files before removing any previous completion marker.
	files, err := readS3ExportFiles(options)
	if err != nil {
		return S3Manifest{}, err
	}

	meta, err := retryS3Operation(
		ctx,
		options.MaxRetries,
		func() (storage.SnapshotMeta, error) {
			return checkSnapshotReadyForExport(ctx, snapshotStorage, snapshotID)
		},
	)
	if err != nil {
		return S3Manifest{}, err
	}
	if options.BlockSize == 0 ||
		(meta.Encryption.GetMode() == types.EncryptionMode_NO_ENCRYPTION &&
			options.DecryptionInfo == nil) {

		// Plaintext CLI exports ignore encryption-specific overrides, while
		// retaining block geometry recorded by snapshot creation.
		options.BlockSize = meta.BlockSize
	}

	if options.MaxRetries != 0 {
		destination = retryingS3Destination{
			S3Destination: destination,
			maxRetries:    options.MaxRetries,
		}
	}

	count, err := s3PartitionCount(meta, options.PartSize)
	if err != nil {
		return S3Manifest{}, err
	}

	exists, err := destination.BucketExists(ctx, options.Bucket)
	if err != nil {
		return S3Manifest{}, fmt.Errorf("checking destination bucket: %w", err)
	}

	if !exists {
		return S3Manifest{}, fmt.Errorf(
			"destination bucket %v does not exist",
			options.Bucket,
		)
	}

	prefix := s3BackupPrefix(options.Prefix)
	uri := "s3://" + options.Bucket + "/" + prefix
	manifestKey := prefix + "manifest.json"

	logging.Info(ctx, "exporting raw backup to %v", uri)

	interpretation := "plaintext"
	if meta.Encryption.GetMode() == types.EncryptionMode_ENCRYPTION_AES_XTS ||
		options.DecryptionInfo != nil {
		interpretation = "ciphertext"
	}
	blockSize := options.BlockSize
	var kekID string
	var encryptedDEK string
	if meta.Encryption.GetMode() != types.EncryptionMode_NO_ENCRYPTION ||
		options.DecryptionInfo != nil {
		blockSize = options.BlockSize
		kekID = options.KekID
		if options.DecryptionInfo != nil {
			encryptedDEK = options.DecryptionInfo.EncryptedDEK
		} else {
			encryptedDEK = base64.StdEncoding.EncodeToString(options.EncryptedDEK)
		}
	}

	if !state.Initialized {
		backupUUID, err := uuid.NewV4()
		if err != nil {
			return S3Manifest{}, err
		}
		state.Manifest = S3Manifest{
			FormatVersion:         1,
			Format:                "partitioned-raw-image",
			Compression:           options.Compression,
			SnapshotID:            snapshotID,
			BackupUUID:            backupUUID.String(),
			CreatedAt:             time.Now().UTC(),
			Size:                  meta.Size,
			BlockSize:             blockSize,
			KekID:                 kekID,
			EncryptedDEK:          encryptedDEK,
			DecryptionInfo:        options.DecryptionInfo,
			EncryptionMode:        meta.Encryption.GetMode().String(),
			EncryptionKeyHash:     meta.Encryption.GetKeyHash(),
			RawDataInterpretation: interpretation,
			MaxPartSize:           options.PartSize,
			PartCount:             count,
			Parts:                 make([]S3ManifestPart, 0),
		}
		state.Bucket = options.Bucket
		state.Prefix = prefix
		state.Initialized = true
		if err := transferMetrics.saveCheckpoint(ctx, saveState); err != nil {
			return state.Manifest, err
		}
	}
	manifest := &state.Manifest
	if err := validateS3ResumeState(
		*manifest,
		snapshotID,
		meta,
		options,
		count,
	); err != nil {
		return *manifest, task_errors.NewNonRetriableError(err)
	}

	encryptedDEKData, err := base64.StdEncoding.DecodeString(manifest.EncryptedDEK)
	if err != nil {
		return *manifest, task_errors.NewNonRetriableErrorf(
			"encrypted_dek must be valid Base64",
		)
	}

	checkpoint, err := openS3Checkpoint(
		ctx, destination, options, meta, manifest,
	)
	if err != nil {
		return S3Manifest{}, err
	}

	// Preserve CLI file checkpoints alongside the task's durable state callback.
	saveTaskState := saveState
	saveState = func(ctx context.Context) error {
		return transferMetrics.saveCheckpoint(
			ctx,
			func(ctx context.Context) error {
				if err := checkpoint.save(*manifest); err != nil {
					return err
				}

				return saveTaskState(ctx)
			},
		)
	}

	// Validate and persist even an empty checkpoint before changing objects.
	if err := saveState(ctx); err != nil {
		return *manifest, err
	}

	if !state.DestinationPrepared {
		exists, err = destination.ObjectExists(ctx, options.Bucket, manifestKey)
		if err != nil {
			return S3Manifest{}, fmt.Errorf(
				"checking destination manifest: %w", err,
			)
		}

		if exists {
			// A failed overwrite must not leave the old marker visible.
			err = destination.DeleteObject(ctx, options.Bucket, manifestKey)
			if err != nil {
				return S3Manifest{}, fmt.Errorf(
					"removing previous destination manifest: %w", err,
				)
			}
		}

		state.DestinationPrepared = true
		if err := saveState(ctx); err != nil {
			return *manifest, err
		}
	}

	if len(manifest.Parts) != 0 {
		logging.Info(ctx, "resuming backup %v: reusing %v/%v completed parts", uri, len(manifest.Parts), count)
	}
	copyStarted := time.Now()
	progress := &s3ExportProgress{metrics: transferMetrics}
	for _, part := range manifest.Parts {
		progress.restoreCompleted(part.Length, part.StoredLength)
	}

	stopProgress := progress.startLogging(
		ctx,
		uri,
		meta.Size,
		count,
		copyStarted,
	)
	defer stopProgress()
	err = exportS3Parts(
		ctx,
		snapshotStorage,
		destination,
		meta,
		options,
		prefix,
		manifest,
		progress,
		copyStarted,
		saveState,
	)
	if err != nil {
		return *manifest, err
	}

	if err := validateS3Manifest(*manifest); err != nil {
		return *manifest, fmt.Errorf("backup %v: %w", uri, err)
	}

	if len(manifest.EncryptedDEK) != 0 {
		// The manifest preserves Base64 text; the companion file contains
		// the decoded encrypted DEK bytes for unpack compatibility.
		err := destination.PutObjectWithOptions(
			ctx,
			options.Bucket,
			prefix+"encrypted.dek",
			persistence.S3Object{Data: encryptedDEKData},
			persistence.S3PutObjectOptions{
				ContentType: "application/octet-stream",
				ContentMD5:  true,
			},
		)
		if err != nil {
			return *manifest, fmt.Errorf("backup %v uploading encrypted.dek: %w", uri, err)
		}
	}

	if manifest.KekID != "" {
		err := destination.PutObjectWithOptions(
			ctx,
			options.Bucket,
			prefix+"kek.id",
			persistence.S3Object{Data: []byte(manifest.KekID)},
			persistence.S3PutObjectOptions{
				ContentType: "text/plain",
				ContentMD5:  true,
			},
		)
		if err != nil {
			return *manifest, fmt.Errorf("backup %v uploading kek.id: %w", uri, err)
		}
	}

	for _, file := range files {
		// Recovery tools are shared by all backups in the bucket.
		// PUT replaces the existing object without a delete/create gap.
		progress.setStage("uploading " + file.key)
		err := destination.PutObjectWithOptions(
			ctx,
			options.Bucket,
			file.key,
			persistence.S3Object{Data: file.data},
			persistence.S3PutObjectOptions{
				ContentType: file.contentType,
				ContentMD5:  true,
			},
		)
		if err != nil {
			return *manifest, fmt.Errorf("backup %v uploading %v: %w", uri, file.key, err)
		}
	}

	if err := ctx.Err(); err != nil {
		return *manifest, fmt.Errorf(
			"backup %v stopped before manifest publication: %w",
			uri,
			err,
		)
	}

	data, err := marshalS3Manifest(*manifest, MaxS3ManifestSize)
	if err != nil {
		return *manifest, fmt.Errorf("backup %v: %w", uri, err)
	}

	progress.setStage("publishing manifest")
	err = destination.PutObjectWithOptions(
		ctx,
		options.Bucket,
		manifestKey,
		persistence.S3Object{Data: data},
		persistence.S3PutObjectOptions{
			ContentType: "application/json",
			ContentMD5:  true,
		},
	)
	if err != nil {
		// Even a permanent final response can follow a retry after a successful
		// write whose response was lost. Confirm the intended bytes on any
		// error.
		stored, readErr := destination.GetObjectWithLimit(
			ctx,
			options.Bucket,
			manifestKey,
			int64(len(data)),
		)
		if readErr == nil && !bytes.Equal(stored, data) {
			readErr = fmt.Errorf(
				"stored manifest does not match intended content",
			)
		}

		if readErr != nil {
			return *manifest, fmt.Errorf(
				"manifest publication is uncertain at %vmanifest.json: "+
					"upload: %w; verification: %v",
				uri,
				err,
				readErr,
			)
		}
	}

	logging.Info(ctx, "raw backup complete: %vmanifest.json", uri)
	state.Completed = true
	if err := saveState(ctx); err != nil {
		return *manifest, err
	}

	transferMetrics.completed.Inc()
	return *manifest, nil
}
