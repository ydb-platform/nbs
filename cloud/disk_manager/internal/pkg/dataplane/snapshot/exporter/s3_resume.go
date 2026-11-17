package exporter

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
)

func validateS3ResumeState(
	manifest S3Manifest,
	snapshotID string,
	meta storage.SnapshotMeta,
	options S3ExportOptions,
	count uint32,
) error {

	decryption, _ := json.Marshal(options.DecryptionInfo)
	savedDecryption, _ := json.Marshal(manifest.DecryptionInfo)
	if manifest.FormatVersion != 1 ||
		manifest.Format != "partitioned-raw-image" ||
		manifest.SnapshotID != snapshotID || manifest.Size != meta.Size ||
		manifest.MaxPartSize != options.PartSize ||
		manifest.PartCount != count ||
		manifest.Compression != options.Compression ||
		manifest.BlockSize != options.BlockSize ||
		manifest.EncryptionMode != meta.Encryption.GetMode().String() ||
		!bytes.Equal(
			manifest.EncryptionKeyHash, meta.Encryption.GetKeyHash(),
		) ||
		!bytes.Equal(decryption, savedDecryption) {

		return fmt.Errorf(
			"source or export parameters changed since checkpoint",
		)
	}

	seen := make(map[uint32]bool, len(manifest.Parts))
	for _, part := range manifest.Parts {
		if part.Number == 0 || part.Number > count || seen[part.Number] {
			return fmt.Errorf("invalid checkpoint part number %v", part.Number)
		}

		seen[part.Number] = true
		start, end, err := partitionChunkRange(
			meta.ChunkCount,
			part.Number,
			count,
		)
		if err != nil {
			return err
		}

		length, err := partitionSize(meta.Size, start, end)
		if err != nil {
			return err
		}

		sum, err := hex.DecodeString(part.SHA256)
		if err != nil || len(sum) != 32 ||
			part.Key != s3PartKey(part.Number, options.Compression) ||
			part.Offset != uint64(start)*chunkSize || part.Length != length ||
			part.StoredLength == 0 {

			return fmt.Errorf("invalid checkpoint for part %v", part.Number)
		}
	}

	return nil
}
