package backup

import (
	"context"
	"encoding/json"
	"math"

	"github.com/golang/protobuf/proto"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

// Must match the chunk size used by snapshot creation tasks.
const ChunkSize = 4 * 1024 * 1024

type LoadOptions struct {
	Kind             protos.BackupSourceKind
	ID               string
	DiskID           string
	ExpectedFolderID string
}

type Meta struct {
	ID                string
	FolderID          string
	Size              uint64
	StorageSize       uint64
	EncryptionMode    uint32
	EncryptionKeyHash []byte
}

// Original bytes are retained so restore retries can pin both object hashes.
// The full protobuf chunk map is loaded even when exporting only a partition.
type LoadedBackup struct {
	Meta         Meta
	ChunkIDs     []string
	MetaData     []byte
	ChunkMapData []byte
}

func Load(
	ctx context.Context,
	reader ObjectReader,
	options LoadOptions,
) (*LoadedBackup, error) {

	if options.ID == "" {
		return nil, errors.NewNonRetriableErrorf("backup source id is empty")
	}

	var key string
	switch options.Kind {
	case protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT:
		if options.DiskID == "" {
			return nil, errors.NewNonRetriableErrorf(
				"source disk id is required for backup of snapshot %v",
				options.ID,
			)
		}

		key = SnapshotMetaKey(options.DiskID, options.ID)
	case protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE:
		key = ImageMetaKey(options.ID)
	default:
		return nil, errors.NewNonRetriableErrorf(
			"unknown backup source kind %v",
			options.Kind,
		)
	}

	object, err := reader.GetObject(ctx, key)
	if err != nil {
		// S3 reports NoSuchKey as a silent non-retriable error. The meta
		// is written last and marks completion of the backup.
		if errors.Is(err, errors.NewEmptyNonRetriableError()) &&
			errors.IsSilent(err) {

			return nil, errors.NewSilentNonRetriableErrorf(
				"backup %q is not complete or does not exist: "+
					"completion metadata %q is missing. "+
					"Wait for the backup to finish and retry, or check "+
					"the source IDs and backup S3 configuration",
				options.ID,
				key,
			)
		}

		return nil, err
	}

	var meta Meta
	if options.Kind == protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT {
		var snapshot SnapshotMeta
		err = json.Unmarshal(object.Data, &snapshot)
		if err == nil && snapshot.DiskID != options.DiskID {
			return nil, errors.NewNonRetriableErrorf(
				"%v has disk id %q, expected %q",
				key,
				snapshot.DiskID,
				options.DiskID,
			)
		}

		meta = Meta{
			ID:                snapshot.ID,
			FolderID:          snapshot.FolderID,
			Size:              snapshot.Size,
			StorageSize:       snapshot.StorageSize,
			EncryptionMode:    snapshot.EncryptionMode,
			EncryptionKeyHash: snapshot.EncryptionKeyHash,
		}
	} else {
		var image ImageMeta
		err = json.Unmarshal(object.Data, &image)
		meta = Meta{
			ID:                image.ID,
			FolderID:          image.FolderID,
			Size:              image.Size,
			StorageSize:       image.StorageSize,
			EncryptionMode:    image.EncryptionMode,
			EncryptionKeyHash: image.EncryptionKeyHash,
		}
	}

	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to parse %v: %w",
			key,
			err,
		)
	}

	if meta.ID != options.ID {
		return nil, errors.NewNonRetriableErrorf(
			"meta of backup %v has id %q",
			options.ID,
			meta.ID,
		)
	}

	if options.ExpectedFolderID != "" &&
		meta.FolderID != options.ExpectedFolderID {

		return nil, errors.NewNonRetriableErrorf(
			"backup %v has folder id %q, expected %q",
			options.ID,
			meta.FolderID,
			options.ExpectedFolderID,
		)
	}

	if meta.Size == 0 || meta.Size%ChunkSize != 0 {
		return nil, errors.NewNonRetriableErrorf(
			"backup %v has size %v, expected non-zero multiple of %v",
			options.ID,
			meta.Size,
			ChunkSize,
		)
	}

	chunkCount := meta.Size / ChunkSize
	if chunkCount > math.MaxUint32 {
		return nil, errors.NewNonRetriableErrorf(
			"backup %v of size %v has too many chunks",
			options.ID,
			meta.Size,
		)
	}

	key = ChunkMapKey(options.ID)
	chunkMapObject, err := reader.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}

	var chunkMap protos.BackupChunkMap
	if err := proto.Unmarshal(chunkMapObject.Data, &chunkMap); err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to parse %v: %w",
			key,
			err,
		)
	}

	if uint64(len(chunkMap.ChunkIds)) != chunkCount {
		return nil, errors.NewNonRetriableErrorf(
			"%v has %v chunks, expected %v",
			key,
			len(chunkMap.ChunkIds),
			chunkCount,
		)
	}

	return &LoadedBackup{
		Meta:         meta,
		ChunkIDs:     chunkMap.ChunkIds,
		MetaData:     object.Data,
		ChunkMapData: chunkMapObject.Data,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

// The copy ends with the meta: a backup without it is not complete, whatever
// the copy reported (a copy of an older build ended well without writing
// it). Reads the meta and checks that it names the snapshot. A retriable
// error of the follower is returned as it is.
func CheckSnapshotMeta(
	ctx context.Context,
	reader ObjectReader,
	diskID string,
	snapshotID string,
) error {

	return checkMeta(ctx, reader, SnapshotMetaKey(diskID, snapshotID), snapshotID)
}

func CheckImageMeta(
	ctx context.Context,
	reader ObjectReader,
	imageID string,
) error {

	return checkMeta(ctx, reader, ImageMetaKey(imageID), imageID)
}

func checkMeta(
	ctx context.Context,
	reader ObjectReader,
	key string,
	id string,
) error {

	object, err := reader.GetObject(ctx, key)
	if err != nil {
		if errors.Is(err, errors.NewEmptyRetriableError()) {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"backup of %v is not complete: no readable %v in the follower: %w",
			id,
			key,
			err,
		)
	}

	var meta struct {
		ID string `json:"id"`
	}
	err = json.Unmarshal(object.Data, &meta)
	if err != nil {
		return errors.NewNonRetriableErrorf(
			"backup of %v is not complete: failed to parse %v: %w",
			id,
			key,
			err,
		)
	}

	if meta.ID != id {
		return errors.NewNonRetriableErrorf(
			"backup of %v is not complete: %v names %q",
			id,
			key,
			meta.ID,
		)
	}

	return nil
}
