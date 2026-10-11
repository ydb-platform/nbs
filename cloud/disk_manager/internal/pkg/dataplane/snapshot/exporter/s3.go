package exporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/gofrs/uuid"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

const DefaultS3PartSize = 256 * 1024 * 1024
const MaxS3PartSize = 1024 * 1024 * 1024

// S3Destination is the narrow destination API needed by raw export.
type S3Destination interface {
	BucketExists(context.Context, string) (bool, error)
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
	Bucket          string
	Prefix          string
	PartSize        uint64
	ReadWorkerCount int
}

// S3Manifest is the completion marker. Parts are reconstructed in this order,
// verifying each length and SHA-256; object listing order and ETags are
// irrelevant.
type S3Manifest struct {
	FormatVersion  uint32    `json:"format_version"`
	Format         string    `json:"format"`
	SnapshotID     string    `json:"snapshot_id"`
	BackupUUID     string    `json:"backup_uuid"`
	CreatedAt      time.Time `json:"created_at"`
	Size           uint64    `json:"size"`
	EncryptionMode string    `json:"encryption_mode"`
	// Optional base64-encoded disk key hash, never secret key material.
	EncryptionKeyHash []byte `json:"encryption_key_hash,omitempty"`

	RawDataInterpretation string           `json:"raw_data_interpretation"`
	MaxPartSize           uint64           `json:"max_part_size"`
	PartCount             uint32           `json:"part_count"`
	Parts                 []S3ManifestPart `json:"parts"`
}

type S3ManifestPart struct {
	Number uint32 `json:"number"`
	Key    string `json:"key"`
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
	SHA256 string `json:"sha256"`
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

func s3BackupPrefix(
	parent string,
	snapshotID string,
	backupUUID string,
) string {

	prefix := strings.Trim(parent, "/")
	if prefix != "" {
		prefix += "/"
	}

	return prefix + snapshotID + "-backup-" + backupUUID + "/"
}

// The bounded part writer prevents an unexpected source stream from growing
// beyond the planned partition. One buffer is reused after PutObject returns,
// including all SDK retries. Chunk reads additionally retain up to workers * 4
// pending reads/results and the selected partition's chunk map. Backup sources
// additionally retain their full chunk map.
type boundedPartWriter struct {
	buffer    *bytes.Buffer
	remaining uint64
}

func (w *boundedPartWriter) Write(data []byte) (int, error) {
	if uint64(len(data)) > w.remaining {
		return 0, fmt.Errorf("raw export exceeds planned part length")
	}

	n, err := w.buffer.Write(data)
	w.remaining -= uint64(n)
	return n, err
}

func validateS3Manifest(manifest S3Manifest) error {
	if uint64(len(manifest.Parts)) != uint64(manifest.PartCount) {
		return fmt.Errorf("manifest part count mismatch")
	}

	var offset uint64
	for i, part := range manifest.Parts {
		sum, err := hex.DecodeString(part.SHA256)
		if part.Number != uint32(i+1) ||
			part.Key != fmt.Sprintf("part-%08d.raw", i+1) ||
			part.Offset != offset ||
			part.Length == 0 ||
			part.Length > manifest.MaxPartSize ||
			offset > manifest.Size ||
			part.Length > manifest.Size-offset ||
			err != nil ||
			len(sum) != sha256.Size {
			return fmt.Errorf("invalid manifest part %v", i+1)
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

// S3Export is one export of a ready snapshot to an S3 prefix, split into
// parts so that a task can save its progress between them. Parts are
// independent: ExportPart(n) reads chunks of part n only and uploads it under
// a key that depends on the backup UUID, so a part interrupted by a restart
// is overwritten under the same key when ExportPart(n) is called again.
type S3Export struct {
	snapshotStorage Source
	snapshotID      string
	meta            storage.SnapshotMeta
	destination     S3Destination
	options         S3ExportOptions
	backupUUID      string
	prefix          string
	uri             string
	partCount       uint32
	buffer          bytes.Buffer
}

// NewS3Export validates the options, reads the snapshot meta, checks the
// destination bucket and plans the parts. backupUUID names the prefix; a task
// keeps it in its state so that every attempt writes to the same prefix.
func NewS3Export(
	ctx context.Context,
	snapshotStorage Source,
	snapshotID string,
	destination S3Destination,
	options S3ExportOptions,
	backupUUID string,
) (*S3Export, error) {

	if snapshotID == "" || options.Bucket == "" || backupUUID == "" {
		return nil, fmt.Errorf(
			"snapshot ID, destination bucket and backup UUID are required",
		)
	}

	if err := ValidateReadWorkers(options.ReadWorkerCount); err != nil {
		return nil, err
	}

	if err := ValidateS3PartSize(options.PartSize); err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	meta, err := checkSnapshotReadyForExport(ctx, snapshotStorage, snapshotID)
	if err != nil {
		return nil, err
	}

	count, err := s3PartitionCount(meta, options.PartSize)
	if err != nil {
		return nil, err
	}

	exists, err := destination.BucketExists(ctx, options.Bucket)
	if err != nil {
		return nil, fmt.Errorf("checking destination bucket: %w", err)
	}

	if !exists {
		return nil, fmt.Errorf(
			"destination bucket %v does not exist",
			options.Bucket,
		)
	}

	prefix := s3BackupPrefix(options.Prefix, snapshotID, backupUUID)
	return &S3Export{
		snapshotStorage: snapshotStorage,
		snapshotID:      snapshotID,
		meta:            meta,
		destination:     destination,
		options:         options,
		backupUUID:      backupUUID,
		prefix:          prefix,
		uri:             "s3://" + options.Bucket + "/" + prefix,
		partCount:       count,
	}, nil
}

func (e *S3Export) PartCount() uint32 {
	return e.partCount
}

func (e *S3Export) Size() uint64 {
	return e.meta.Size
}

func (e *S3Export) ChunkCount() uint32 {
	return e.meta.ChunkCount
}

// Prefix of every exported object, ends with "/".
func (e *S3Export) Prefix() string {
	return e.prefix
}

func (e *S3Export) ManifestKey() string {
	return e.prefix + "manifest.json"
}

// ExportPart reads the 1-based part and uploads it. The returned part carries
// the SHA-256 of the uploaded bytes for the manifest.
func (e *S3Export) ExportPart(
	ctx context.Context,
	number uint32,
) (S3ManifestPart, error) {

	if number == 0 || number > e.partCount {
		return S3ManifestPart{}, task_errors.NewNonRetriableErrorf(
			"backup %v has no part %v of %v",
			e.uri,
			number,
			e.partCount,
		)
	}

	if err := ctx.Err(); err != nil {
		return S3ManifestPart{}, fmt.Errorf(
			"backup %v stopped before part %v/%v: %w",
			e.uri,
			number,
			e.partCount,
			err,
		)
	}

	start, end, err := partitionChunkRange(
		e.meta.ChunkCount,
		number,
		e.partCount,
	)
	if err != nil {
		return S3ManifestPart{}, fmt.Errorf(
			"backup %v planning part %v: %w",
			e.uri,
			number,
			err,
		)
	}

	length, err := partitionSize(e.meta.Size, start, end)
	if err != nil || length > e.options.PartSize {
		return S3ManifestPart{}, fmt.Errorf(
			"backup %v has invalid part %v length %v: %v",
			e.uri,
			number,
			length,
			err,
		)
	}

	e.buffer.Reset()
	e.buffer.Grow(int(length))
	writer := boundedPartWriter{buffer: &e.buffer, remaining: length}
	stats, err := exportPartitionToWriter(
		ctx,
		e.snapshotStorage,
		e.snapshotID,
		e.meta,
		&writer,
		number,
		e.partCount,
		e.options.ReadWorkerCount,
	)
	if err != nil {
		return S3ManifestPart{}, fmt.Errorf(
			"backup %v reading part %v/%v: %w",
			e.uri,
			number,
			e.partCount,
			err,
		)
	}

	if writer.remaining != 0 || stats.Size != length {
		return S3ManifestPart{}, fmt.Errorf(
			"backup %v part %v byte count mismatch",
			e.uri,
			number,
		)
	}

	key := fmt.Sprintf("part-%08d.raw", number)
	sum := sha256.Sum256(e.buffer.Bytes())
	err = e.destination.PutObjectWithOptions(
		ctx,
		e.options.Bucket,
		e.prefix+key,
		persistence.S3Object{Data: e.buffer.Bytes()},
		persistence.S3PutObjectOptions{
			ContentType: "application/octet-stream",
			ContentMD5:  true,
		},
	)
	if err != nil {
		return S3ManifestPart{}, fmt.Errorf(
			"backup %v uploading part %v/%v: %w",
			e.uri,
			number,
			e.partCount,
			err,
		)
	}

	logging.Info(
		ctx,
		"backup %v: uploaded part %v/%v, %v bytes",
		e.uri,
		number,
		e.partCount,
		length,
	)

	return S3ManifestPart{
		Number: number,
		Key:    key,
		Offset: uint64(start) * chunkSize,
		Length: length,
		SHA256: hex.EncodeToString(sum[:]),
	}, nil
}

// PublishManifest validates the parts against the plan and writes the
// completion marker. An uncertain upload is confirmed by reading the object
// back, so a lost response after a successful write does not fail the export.
func (e *S3Export) PublishManifest(
	ctx context.Context,
	parts []S3ManifestPart,
) (S3Manifest, error) {

	interpretation := "plaintext"
	if e.meta.Encryption.GetMode() == types.EncryptionMode_ENCRYPTION_AES_XTS {
		interpretation = "ciphertext"
	}

	manifest := S3Manifest{
		FormatVersion:         1,
		Format:                "partitioned-raw-image",
		SnapshotID:            e.snapshotID,
		BackupUUID:            e.backupUUID,
		CreatedAt:             time.Now().UTC(),
		Size:                  e.meta.Size,
		EncryptionMode:        e.meta.Encryption.GetMode().String(),
		EncryptionKeyHash:     e.meta.Encryption.GetKeyHash(),
		RawDataInterpretation: interpretation,
		MaxPartSize:           e.options.PartSize,
		PartCount:             e.partCount,
		Parts:                 append(make([]S3ManifestPart, 0, len(parts)), parts...),
	}

	if err := validateS3Manifest(manifest); err != nil {
		return manifest, fmt.Errorf("backup %v: %w", e.uri, err)
	}

	if err := ctx.Err(); err != nil {
		return manifest, fmt.Errorf(
			"backup %v stopped before manifest publication: %w",
			e.uri,
			err,
		)
	}

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return manifest, fmt.Errorf("backup %v: %w", e.uri, err)
	}

	manifestKey := e.ManifestKey()
	err = e.destination.PutObjectWithOptions(
		ctx,
		e.options.Bucket,
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
		stored, readErr := e.destination.GetObjectWithLimit(
			ctx,
			e.options.Bucket,
			manifestKey,
			int64(len(data)),
		)
		if readErr == nil && !bytes.Equal(stored, data) {
			readErr = fmt.Errorf(
				"stored manifest does not match intended content",
			)
		}

		if readErr != nil {
			return manifest, fmt.Errorf(
				"manifest publication is uncertain at %v: "+
					"upload: %w; verification: %v",
				e.options.Bucket+"/"+manifestKey,
				err,
				readErr,
			)
		}
	}

	logging.Info(ctx, "snapshot backup complete: %v", e.uri+"manifest.json")
	return manifest, nil
}

// ExportToS3 synchronously exports a complete ready snapshot, publishing its
// completion manifest only after every raw part succeeds. Source access is
// read-only; the caller must keep the source available throughout the export.
func ExportToS3(
	ctx context.Context,
	snapshotStorage Source,
	snapshotID string,
	destination S3Destination,
	options S3ExportOptions,
) (S3Manifest, error) {

	backupUUID, err := uuid.NewV4()
	if err != nil {
		return S3Manifest{}, err
	}

	export, err := NewS3Export(
		ctx,
		snapshotStorage,
		snapshotID,
		destination,
		options,
		backupUUID.String(),
	)
	if err != nil {
		return S3Manifest{}, err
	}

	logging.Info(ctx, "exporting snapshot backup to %v", export.uri)

	parts := make([]S3ManifestPart, 0, export.partCount)
	var uploaded uint64
	for number := uint32(1); number <= export.partCount; number++ {
		part, err := export.ExportPart(ctx, number)
		if err != nil {
			return S3Manifest{}, err
		}

		parts = append(parts, part)
		uploaded += part.Length
		logging.Info(
			ctx,
			"backup %v: uploaded %v/%v bytes, part %v/%v",
			export.uri,
			uploaded,
			export.meta.Size,
			number,
			export.partCount,
		)
	}

	return export.PublishManifest(ctx, parts)
}
