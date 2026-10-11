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

	if snapshotID == "" || options.Bucket == "" {
		return S3Manifest{}, fmt.Errorf(
			"snapshot ID and destination bucket are required",
		)
	}

	if err := ValidateReadWorkers(options.ReadWorkerCount); err != nil {
		return S3Manifest{}, err
	}

	if err := ValidateS3PartSize(options.PartSize); err != nil {
		return S3Manifest{}, err
	}

	if err := ctx.Err(); err != nil {
		return S3Manifest{}, err
	}

	meta, err := checkSnapshotReadyForExport(ctx, snapshotStorage, snapshotID)
	if err != nil {
		return S3Manifest{}, err
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

	backupUUID, err := uuid.NewV4()
	if err != nil {
		return S3Manifest{}, err
	}

	prefix := s3BackupPrefix(options.Prefix, snapshotID, backupUUID.String())
	uri := "s3://" + options.Bucket + "/" + prefix
	logging.Info(ctx, "exporting snapshot backup to %v", uri)

	interpretation := "plaintext"
	if meta.Encryption.GetMode() == types.EncryptionMode_ENCRYPTION_AES_XTS {
		interpretation = "ciphertext"
	}

	manifest := S3Manifest{
		FormatVersion:         1,
		Format:                "partitioned-raw-image",
		SnapshotID:            snapshotID,
		BackupUUID:            backupUUID.String(),
		CreatedAt:             time.Now().UTC(),
		Size:                  meta.Size,
		EncryptionMode:        meta.Encryption.GetMode().String(),
		EncryptionKeyHash:     meta.Encryption.GetKeyHash(),
		RawDataInterpretation: interpretation,
		MaxPartSize:           options.PartSize,
		PartCount:             count,
		Parts:                 make([]S3ManifestPart, 0),
	}

	var buffer bytes.Buffer
	var uploaded uint64
	// The uint64 iteration handles the maximum uint32 part count without
	// wrapping.
	for number := uint64(1); number <= uint64(count); number++ {
		if err := ctx.Err(); err != nil {
			return manifest, fmt.Errorf(
				"backup %v stopped before part %v/%v: %w",
				uri,
				number,
				count,
				err,
			)
		}

		start, end, err := partitionChunkRange(
			meta.ChunkCount,
			uint32(number),
			count,
		)
		if err != nil {
			return manifest, fmt.Errorf(
				"backup %v planning part %v: %w",
				uri,
				number,
				err,
			)
		}

		length, err := partitionSize(meta.Size, start, end)
		if err != nil || length > options.PartSize {
			return manifest, fmt.Errorf(
				"backup %v has invalid part %v length %v: %v",
				uri,
				number,
				length,
				err,
			)
		}

		buffer.Reset()
		buffer.Grow(int(length))
		writer := boundedPartWriter{buffer: &buffer, remaining: length}
		stats, err := exportPartitionToWriter(
			ctx,
			snapshotStorage,
			snapshotID,
			meta,
			&writer,
			uint32(number),
			count,
			options.ReadWorkerCount,
		)
		if err != nil {
			return manifest, fmt.Errorf(
				"backup %v reading part %v/%v: %w",
				uri,
				number,
				count,
				err,
			)
		}

		if writer.remaining != 0 || stats.Size != length {
			return manifest, fmt.Errorf(
				"backup %v part %v byte count mismatch",
				uri,
				number,
			)
		}

		key := fmt.Sprintf("part-%08d.raw", number)
		sum := sha256.Sum256(buffer.Bytes())
		err = destination.PutObjectWithOptions(
			ctx,
			options.Bucket,
			prefix+key,
			persistence.S3Object{Data: buffer.Bytes()},
			persistence.S3PutObjectOptions{
				ContentType: "application/octet-stream",
				ContentMD5:  true,
			},
		)
		if err != nil {
			return manifest, fmt.Errorf(
				"backup %v uploading part %v/%v: %w",
				uri,
				number,
				count,
				err,
			)
		}

		manifest.Parts = append(
			manifest.Parts,
			S3ManifestPart{
				Number: uint32(number),
				Key:    key,
				Offset: uint64(start) * chunkSize,
				Length: length,
				SHA256: hex.EncodeToString(sum[:]),
			},
		)
		uploaded += length
		logging.Info(
			ctx,
			"backup %v: uploaded %v/%v bytes, part %v/%v",
			uri,
			uploaded,
			meta.Size,
			number,
			count,
		)
	}

	if err := validateS3Manifest(manifest); err != nil {
		return manifest, fmt.Errorf("backup %v: %w", uri, err)
	}

	if err := ctx.Err(); err != nil {
		return manifest, fmt.Errorf(
			"backup %v stopped before manifest publication: %w",
			uri,
			err,
		)
	}

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return manifest, fmt.Errorf("backup %v: %w", uri, err)
	}

	manifestKey := prefix + "manifest.json"
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
			return manifest, fmt.Errorf(
				"manifest publication is uncertain at %vmanifest.json: "+
					"upload: %w; verification: %v",
				uri,
				err,
				readErr,
			)
		}
	}

	logging.Info(ctx, "snapshot backup complete: %vmanifest.json", uri)
	return manifest, nil
}
