package resources

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type imageStatus uint32

func (s *imageStatus) UnmarshalYDB(res persistence.RawValue) error {
	*s = imageStatus(res.Int64())
	return nil
}

// NOTE: These values are stored in DB, do not shuffle them around.
const (
	imageStatusCreating imageStatus = iota
	imageStatusReady    imageStatus = iota
	imageStatusDeleting imageStatus = iota
	imageStatusDeleted  imageStatus = iota
)

func imageStatusToString(status imageStatus) string {
	switch status {
	case imageStatusCreating:
		return "creating"
	case imageStatusReady:
		return "ready"
	case imageStatusDeleting:
		return "deleting"
	case imageStatusDeleted:
		return "deleted"
	}

	return fmt.Sprintf("unknown_%v", status)
}

////////////////////////////////////////////////////////////////////////////////

// This is mapped into a DB row. If you change this struct, make sure to update
// the mapping code.
type imageState struct {
	id                string
	folderID          string
	srcDiskID         string
	checkpointID      string
	srcImageID        string
	srcSnapshotID     string
	createRequest     []byte
	createTaskID      string
	creatingAt        time.Time
	createdAt         time.Time
	createdBy         string
	deleteTaskID      string
	deletingAt        time.Time
	deletedAt         time.Time
	useDataplaneTasks bool
	size              uint64
	storageSize       uint64
	encryptionMode    uint32
	encryptionKeyHash []byte
	backupCompleted   bool

	status imageStatus
}

func (s *imageState) toImageMeta() *ImageMeta {
	// TODO: Image.CreateRequest should be []byte, because we can't unmarshal
	// it here, without knowing particular protobuf message type.
	return &ImageMeta{
		ID:                s.id,
		FolderID:          s.folderID,
		SrcDiskID:         s.srcDiskID,
		CheckpointID:      s.checkpointID,
		SrcImageID:        s.srcImageID,
		SrcSnapshotID:     s.srcSnapshotID,
		CreateTaskID:      s.createTaskID,
		CreatingAt:        s.creatingAt,
		CreatedBy:         s.createdBy,
		DeleteTaskID:      s.deleteTaskID,
		UseDataplaneTasks: s.useDataplaneTasks,
		Size:              s.size,
		StorageSize:       s.storageSize,
		Encryption: &types.EncryptionDesc{
			Mode: types.EncryptionMode(s.encryptionMode),
			Key: &types.EncryptionDesc_KeyHash{
				KeyHash: s.encryptionKeyHash,
			},
		},
		Ready:           s.status == imageStatusReady,
		BackupCompleted: s.backupCompleted,
	}
}

func (s *imageState) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("id", persistence.UTF8Value(s.id)),
		persistence.StructFieldValue("folder_id", persistence.UTF8Value(s.folderID)),
		persistence.StructFieldValue("src_disk_id", persistence.UTF8Value(s.srcDiskID)),
		persistence.StructFieldValue("checkpoint_id", persistence.UTF8Value(s.checkpointID)),
		persistence.StructFieldValue("src_image_id", persistence.UTF8Value(s.srcImageID)),
		persistence.StructFieldValue("src_snapshot_id", persistence.UTF8Value(s.srcSnapshotID)),
		persistence.StructFieldValue("create_request", persistence.StringValue(s.createRequest)),
		persistence.StructFieldValue("create_task_id", persistence.UTF8Value(s.createTaskID)),
		persistence.StructFieldValue("creating_at", persistence.TimestampValue(s.creatingAt)),
		persistence.StructFieldValue("created_at", persistence.TimestampValue(s.createdAt)),
		persistence.StructFieldValue("created_by", persistence.UTF8Value(s.createdBy)),
		persistence.StructFieldValue("delete_task_id", persistence.UTF8Value(s.deleteTaskID)),
		persistence.StructFieldValue("deleting_at", persistence.TimestampValue(s.deletingAt)),
		persistence.StructFieldValue("deleted_at", persistence.TimestampValue(s.deletedAt)),
		persistence.StructFieldValue("use_dataplane_tasks", persistence.BoolValue(true)), // legacy
		persistence.StructFieldValue("size", persistence.Uint64Value(s.size)),
		persistence.StructFieldValue("storage_size", persistence.Uint64Value(s.storageSize)),
		persistence.StructFieldValue("encryption_mode", persistence.Uint32Value(s.encryptionMode)),
		persistence.StructFieldValue("encryption_keyhash", persistence.StringValue(s.encryptionKeyHash)),
		persistence.StructFieldValue(
			"backup_completed",
			persistence.BoolValue(s.backupCompleted),
		),
		persistence.StructFieldValue("status", persistence.Int64Value(int64(s.status))),
	)
}

func scanImageState(res persistence.Result) (state imageState, err error) {
	err = res.ScanNamed(
		persistence.OptionalWithDefault("id", &state.id),
		persistence.OptionalWithDefault("folder_id", &state.folderID),
		persistence.OptionalWithDefault("src_disk_id", &state.srcDiskID),
		persistence.OptionalWithDefault("checkpoint_id", &state.checkpointID),
		persistence.OptionalWithDefault("src_image_id", &state.srcImageID),
		persistence.OptionalWithDefault("src_snapshot_id", &state.srcSnapshotID),
		persistence.OptionalWithDefault("create_request", &state.createRequest),
		persistence.OptionalWithDefault("create_task_id", &state.createTaskID),
		persistence.OptionalWithDefault("creating_at", &state.creatingAt),
		persistence.OptionalWithDefault("created_at", &state.createdAt),
		persistence.OptionalWithDefault("created_by", &state.createdBy),
		persistence.OptionalWithDefault("delete_task_id", &state.deleteTaskID),
		persistence.OptionalWithDefault("deleting_at", &state.deletingAt),
		persistence.OptionalWithDefault("deleted_at", &state.deletedAt),
		persistence.OptionalWithDefault("use_dataplane_tasks", &state.useDataplaneTasks),
		persistence.OptionalWithDefault("status", &state.status),
		persistence.OptionalWithDefault("size", &state.size),
		persistence.OptionalWithDefault("storage_size", &state.storageSize),
		persistence.OptionalWithDefault("encryption_mode", &state.encryptionMode),
		persistence.OptionalWithDefault("encryption_keyhash", &state.encryptionKeyHash),
		persistence.OptionalWithDefault(
			"backup_completed",
			&state.backupCompleted,
		),
	)
	return
}

func scanImageStates(
	ctx context.Context,
	res persistence.Result,
) ([]imageState, error) {

	var states []imageState
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			state, err := scanImageState(res)
			if err != nil {
				return nil, err
			}

			states = append(states, state)
		}
	}

	return states, nil
}

func imageStateStructTypeString() string {
	return `Struct<
		id: Utf8,
		folder_id: Utf8,
		src_disk_id: Utf8,
		checkpoint_id: Utf8,
		src_image_id: Utf8,
		src_snapshot_id: Utf8,
		create_request: String,
		create_task_id: Utf8,
		creating_at: Timestamp,
		created_at: Timestamp,
		created_by: Utf8,
		delete_task_id: Utf8,
		deleting_at: Timestamp,
		deleted_at: Timestamp,
		use_dataplane_tasks: Bool, /* legacy */
		size: Uint64,
		storage_size: Uint64,
		encryption_mode: Uint32,
		encryption_keyhash: String,
		backup_completed: Bool,
		status: Int64>`
}

func imageStateTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("folder_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("src_disk_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("checkpoint_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("src_image_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("src_snapshot_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("create_request", persistence.Optional(persistence.TypeString)),
		persistence.WithColumn("create_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("creating_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("created_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("created_by", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("delete_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("deleting_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("deleted_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("use_dataplane_tasks", persistence.Optional(persistence.TypeBool)), // legacy
		persistence.WithColumn("size", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("storage_size", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("encryption_mode", persistence.Optional(persistence.TypeUint32)),
		persistence.WithColumn("encryption_keyhash", persistence.Optional(persistence.TypeString)),
		persistence.WithColumn(
			"backup_completed",
			persistence.Optional(persistence.TypeBool),
		),
		persistence.WithColumn("status", persistence.Optional(persistence.TypeInt64)),
		persistence.WithPrimaryKeyColumn("id"),
	)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) imageExists(
	ctx context.Context,
	tx *persistence.Transaction,
	imageID string,
) (bool, error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select count(*)
		from images
		where id = $id
	`, s.imagesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(imageID)),
	)
	if err != nil {
		return false, err
	}
	defer res.Close()

	var count uint64
	if !res.NextResultSet(ctx) || !res.NextRow() {
		return false, nil
	}

	err = res.ScanWithDefaults(&count)
	if err != nil {
		return false, err
	}

	return count != 0, nil
}

func (s *storageYDB) getImageState(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
) (*imageState, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from images
		where id = $id
	`, s.imagesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(imageID)),
	)
	if err != nil {
		return nil, err
	}

	defer res.Close()

	states, err := scanImageStates(ctx, res)
	if err != nil {
		return nil, err
	}

	if len(states) != 0 {
		return &states[0], nil
	} else {
		return nil, nil
	}
}

func (s *storageYDB) getImageMeta(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
) (*ImageMeta, error) {

	state, err := s.getImageState(ctx, session, imageID)
	if err != nil {
		return nil, err
	}

	if state == nil {
		return nil, nil
	}

	return state.toImageMeta(), nil
}

func (s *storageYDB) createImage(
	ctx context.Context,
	session *persistence.Session,
	image ImageMeta,
) (ImageMeta, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return ImageMeta{}, err
	}

	defer tx.Rollback(ctx)

	// HACK: see NBS-974 for details.
	snapshotExists, err := s.snapshotExists(ctx, tx, image.ID)
	if err != nil {
		return ImageMeta{}, err
	}

	if snapshotExists {
		err = tx.Commit(ctx)
		if err != nil {
			return ImageMeta{}, err
		}

		return ImageMeta{}, errors.NewNonCancellableErrorf(
			"image with id %v can't be created, because snapshot with id %v already exists",
			image.ID,
			image.ID,
		)
	}

	createRequest, err := proto.Marshal(image.CreateRequest)
	if err != nil {
		return ImageMeta{}, errors.NewNonRetriableErrorf(
			"failed to marshal create request for image with id %v: %w",
			image.ID,
			err,
		)
	}

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from images
		where id = $id
	`, s.imagesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(image.ID)),
	)
	if err != nil {
		return ImageMeta{}, err
	}

	defer res.Close()

	states, err := scanImageStates(ctx, res)
	if err != nil {
		return ImageMeta{}, err
	}

	if len(states) != 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return ImageMeta{}, err
		}

		state := states[0]

		if state.status >= imageStatusDeleting {
			logging.Info(ctx, "can't create already deleting/deleted image with id %v", image.ID)
			return ImageMeta{}, errors.NewSilentNonRetriableErrorf(
				"can't create already deleting/deleted image with id %v",
				image.ID,
			)
		}

		// Check idempotency.
		if bytes.Equal(state.createRequest, createRequest) &&
			state.createTaskID == image.CreateTaskID &&
			state.createdBy == image.CreatedBy {

			return *state.toImageMeta(), nil
		}

		return ImageMeta{}, errors.NewNonCancellableErrorf(
			"image with different params already exists, old=%v, new=%v",
			state,
			image,
		)
	}

	state := imageState{
		id:                image.ID,
		folderID:          image.FolderID,
		srcDiskID:         image.SrcDiskID,
		srcImageID:        image.SrcImageID,
		srcSnapshotID:     image.SrcSnapshotID,
		createRequest:     createRequest,
		createTaskID:      image.CreateTaskID,
		creatingAt:        image.CreatingAt,
		createdBy:         image.CreatedBy,
		useDataplaneTasks: true,

		status: imageStatusCreating,
	}

	encryptionMode, encryptionKeyHash, err := GetEncryptionModeAndKeyHash(
		image.Encryption,
	)
	if err != nil {
		return ImageMeta{}, err
	}

	state.encryptionMode = uint32(encryptionMode)
	state.encryptionKeyHash = encryptionKeyHash

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into images
		select *
		from AS_TABLE($states)
	`, s.imagesPath, imageStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return ImageMeta{}, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return ImageMeta{}, err
	}

	return *state.toImageMeta(), nil
}

func (s *storageYDB) imageCreated(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
	checkpointID string,
	createdAt time.Time,
	imageSize uint64,
	imageStorageSize uint64,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from images
		where id = $id
	`, s.imagesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(imageID)),
	)
	if err != nil {
		return err
	}

	defer res.Close()

	states, err := scanImageStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"image with id %v is not found",
			imageID,
		)
	}

	state := states[0]

	if state.status == imageStatusReady {
		if state.checkpointID != checkpointID {
			return errors.NewNonRetriableErrorf(
				"image with id %v and checkpoint id %v can't be created, "+
					"because image with the same id and another "+
					"checkpoint id %v already exists",
				imageID,
				checkpointID,
				state.checkpointID,
			)
		}

		// Nothing to do.
		return tx.Commit(ctx)
	}

	if state.status != imageStatusCreating {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"image with id %v and status %v can't be created",
			imageID,
			imageStatusToString(state.status),
		)
	}

	state.status = imageStatusReady
	state.checkpointID = checkpointID
	state.createdAt = createdAt
	state.size = imageSize
	state.storageSize = imageStorageSize

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into images
		select *
		from AS_TABLE($states)
	`, s.imagesPath, imageStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return err
	}

	// A new ready image of a backed up folder is queued in the same
	// transaction. Other images are queued by EnqueueImageBackup. The image
	// becomes ready once, so its create task identifies the attempt.
	if s.backupEnabledForFolder(state.folderID) {
		err = s.queueImageBackup(ctx, tx, imageID, state.createTaskID)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) deleteImage(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
	taskID string,
	deletingAt time.Time,
) (*ImageMeta, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback(ctx)

	// HACK: see NBS-974 for details.
	snapshotExists, err := s.snapshotExists(ctx, tx, imageID)
	if err != nil {
		return nil, err
	}

	if snapshotExists {
		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}

		return nil, errors.NewNonCancellableErrorf(
			"image with id %v can't be deleted, because snapshot with id %v already exists",
			imageID,
			imageID,
		)
	}

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from images
		where id = $id
	`, s.imagesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(imageID)),
	)
	if err != nil {
		return nil, err
	}

	defer res.Close()

	states, err := scanImageStates(ctx, res)
	if err != nil {
		return nil, err
	}

	if len(states) == 0 {
		// Should be idempotent.
		return nil, nil
	}

	state := states[0]

	if state.status >= imageStatusDeleting {
		// Image already marked as deleting/deleted.

		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}

		return state.toImageMeta(), nil
	}

	state.id = imageID
	state.status = imageStatusDeleting
	state.deleteTaskID = taskID
	state.deletingAt = deletingAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into images
		select *
		from AS_TABLE($states)
	`, s.imagesPath, imageStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return nil, err
	}

	if s.backupEnabled {
		_, err = tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $image_id as Utf8;

			delete from backup_queue
			where image_id = $image_id
		`, s.imagesPath),
			persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		)
		if err != nil {
			return nil, err
		}

		// Any image may have a copy: it is queued by its folder or by hand.
		// Deleting a copy that does not exist is a no-op.
		_, err = tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $image_id as Utf8;

			upsert into backup_delete_queue (image_id)
			values ($image_id)
		`, s.imagesPath),
			persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		)
		if err != nil {
			return nil, err
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return state.toImageMeta(), nil
}

func (s *storageYDB) imageDeleted(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
	deletedAt time.Time,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from images
		where id = $id
	`, s.imagesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(imageID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanImageStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		// It's possible that image is already collected.
		return tx.Commit(ctx)
	}

	state := states[0]

	if state.status == imageStatusDeleted {
		// Nothing to do.
		return tx.Commit(ctx)
	}

	if state.status != imageStatusDeleting {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"image with id %v and status %v can't be deleted",
			imageID,
			imageStatusToString(state.status),
		)
	}

	state.status = imageStatusDeleted
	state.deletedAt = deletedAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into images
		select *
		from AS_TABLE($states)
	`, s.imagesPath, imageStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	if err != nil {
		return err
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $deleted_at as Timestamp;
		declare $image_id as Utf8;

		upsert into deleted (deleted_at, image_id)
		values ($deleted_at, $image_id)
	`, s.imagesPath),
		persistence.ValueParam("$deleted_at", persistence.TimestampValue(deletedAt)),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) clearDeletedImages(
	ctx context.Context,
	session *persistence.Session,
	deletedBefore time.Time,
	limit int,
) error {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $deleted_before as Timestamp;
		declare $limit as Uint64;

		select *
		from deleted
		where deleted_at < $deleted_before
		limit $limit
	`, s.imagesPath),
		persistence.ValueParam("$deleted_before", persistence.TimestampValue(deletedBefore)),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var (
				deletedAt time.Time
				imageID   string
			)
			err = res.ScanNamed(
				persistence.OptionalWithDefault("deleted_at", &deletedAt),
				persistence.OptionalWithDefault("image_id", &imageID),
			)
			if err != nil {
				return err
			}

			_, err = session.ExecuteRW(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				declare $deleted_at as Timestamp;
				declare $image_id as Utf8;
				declare $status as Int64;

				delete from images
				where id = $image_id and status = $status;

				delete from deleted
				where deleted_at = $deleted_at and image_id = $image_id
			`, s.imagesPath),
				persistence.ValueParam("$deleted_at", persistence.TimestampValue(deletedAt)),
				persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
				persistence.ValueParam("$status", persistence.Int64Value(int64(imageStatusDeleted))),
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *storageYDB) listImages(
	ctx context.Context,
	session *persistence.Session,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	return listResources(
		ctx,
		session,
		s.imagesPath,
		"images",
		folderID,
		creatingBefore,
	)
}

// Inserts an attempt that has no task yet.
func (s *storageYDB) queueImageBackup(
	ctx context.Context,
	tx *persistence.Transaction,
	imageID string,
	backupID string,
) error {

	_, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $backup_id as Utf8;
		declare $enqueued_at as Timestamp;

		upsert into backup_queue (image_id, backup_id, task_id, enqueued_at)
		values ($image_id, $backup_id, "", $enqueued_at)
	`, s.imagesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$backup_id", persistence.UTF8Value(backupID)),
		persistence.ValueParam(
			"$enqueued_at",
			persistence.TimestampValue(time.Now()),
		),
	)
	return err
}

func (s *storageYDB) listImagesToBackup(
	ctx context.Context,
	session *persistence.Session,
	limit int,
) ([]ImageBackupRequest, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $limit as Uint64;

		select image_id, backup_id
		from backup_queue
		where task_id is null or task_id = ""
		order by enqueued_at
		limit $limit
	`, s.imagesPath),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var backups []ImageBackupRequest
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var backup ImageBackupRequest
			err = res.ScanNamed(
				persistence.OptionalWithDefault("image_id", &backup.ImageID),
				persistence.OptionalWithDefault("backup_id", &backup.BackupID),
			)
			if err != nil {
				return nil, err
			}
			backups = append(backups, backup)
		}
	}
	return backups, res.Err()
}

func (s *storageYDB) listScheduledImageBackups(
	ctx context.Context,
	session *persistence.Session,
	limit int,
) ([]ScheduledImageBackup, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $limit as Uint64;

		select image_id, backup_id, task_id
		from backup_queue
		where task_id is not null and task_id != ""
		limit $limit
	`, s.imagesPath),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var backups []ScheduledImageBackup
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var backup ScheduledImageBackup
			err = res.ScanNamed(
				persistence.OptionalWithDefault("image_id", &backup.ImageID),
				persistence.OptionalWithDefault("backup_id", &backup.BackupID),
				persistence.OptionalWithDefault("task_id", &backup.TaskID),
			)
			if err != nil {
				return nil, err
			}
			backups = append(backups, backup)
		}
	}
	return backups, res.Err()
}

func (s *storageYDB) imageBackupScheduled(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
	backupID string,
	taskID string,
) (time.Time, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $backup_id as Utf8;

		select enqueued_at
		from backup_queue
		where image_id = $image_id and backup_id = $backup_id
	`, s.imagesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$backup_id", persistence.UTF8Value(backupID)),
	)
	if err != nil {
		return time.Time{}, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		// The attempt left the queue meanwhile: nothing to mark.
		err = res.Err()
		if err != nil {
			return time.Time{}, err
		}

		return time.Time{}, tx.Commit(ctx)
	}

	var enqueuedAt time.Time
	err = res.ScanNamed(
		persistence.OptionalWithDefault("enqueued_at", &enqueuedAt),
	)
	if err != nil {
		return time.Time{}, err
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $backup_id as Utf8;
		declare $task_id as Utf8;

		update backup_queue
		set task_id = $task_id
		where image_id = $image_id and backup_id = $backup_id
	`, s.imagesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$backup_id", persistence.UTF8Value(backupID)),
		persistence.ValueParam("$task_id", persistence.UTF8Value(taskID)),
	)
	if err != nil {
		return time.Time{}, err
	}

	return enqueuedAt, tx.Commit(ctx)
}

func (s *storageYDB) removeImageFromBackupQueue(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
	backupID string,
) error {

	_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $backup_id as Utf8;

		delete from backup_queue
		where image_id = $image_id and backup_id = $backup_id
	`, s.imagesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$backup_id", persistence.UTF8Value(backupID)),
	)
	return err
}

func (s *storageYDB) enqueueImageBackup(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
	backupID string,
) error {

	if len(imageID) == 0 || len(backupID) == 0 {
		return errors.NewNonRetriableErrorf("empty image or backup ID")
	}

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select * from images where id = $id
	`, s.imagesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(imageID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanImageStates(ctx, res)
	if err != nil {
		return err
	}
	if err = res.Err(); err != nil {
		return err
	}
	if len(states) == 0 {
		return errors.NewNonRetriableErrorf(
			"image with id %v is not found",
			imageID,
		)
	}

	state := states[0]
	if state.backupCompleted {
		return tx.Commit(ctx)
	}
	if state.status != imageStatusReady {
		return errors.NewNonRetriableErrorf(
			"image with id %v and status %v can't be backed up",
			imageID,
			imageStatusToString(state.status),
		)
	}

	// A queued or running attempt already backs the image up: the request
	// joins it.
	queued, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;

		select image_id from backup_queue where image_id = $image_id
	`, s.imagesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
	)
	if err != nil {
		return err
	}
	defer queued.Close()

	found := queued.NextResultSet(ctx) && queued.NextRow()
	if err = queued.Err(); err != nil {
		return err
	}
	if found {
		return tx.Commit(ctx)
	}

	err = s.queueImageBackup(ctx, tx, imageID, backupID)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) imageBackupCompleted(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
) error {

	// A deleting image is not marked: its copy may have found the image
	// already deleted and copied nothing.
	_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;
		declare $ready as Int64;

		update images
		set backup_completed = true
		where id = $id and status = $ready
	`, s.imagesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(imageID)),
		persistence.ValueParam(
			"$ready",
			persistence.Int64Value(int64(imageStatusReady)),
		),
	)
	return err
}

func (s *storageYDB) getImageBackupQueueStats(
	ctx context.Context,
	session *persistence.Session,
) (ImageBackupQueueStats, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";

		select
			count_if(task_id is null or task_id = "") as queued,
			count_if(task_id is not null and task_id != "") as scheduled
		from backup_queue
	`, s.imagesPath))
	if err != nil {
		return ImageBackupQueueStats{}, err
	}
	defer res.Close()

	var stats ImageBackupQueueStats
	if !res.NextResultSet(ctx) || !res.NextRow() {
		return stats, res.Err()
	}

	err = res.ScanNamed(
		persistence.OptionalWithDefault("queued", &stats.Queued),
		persistence.OptionalWithDefault("scheduled", &stats.Scheduled),
	)
	if err != nil {
		return ImageBackupQueueStats{}, err
	}
	return stats, res.Err()
}

func (s *storageYDB) listImageIDsForDeletion(
	ctx context.Context,
	session *persistence.Session,
	limit int,
) ([]string, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $limit as Uint64;

		select image_id
		from backup_delete_queue
		limit $limit
	`, s.imagesPath),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var imageIDs []string

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var imageID string
			err = res.ScanNamed(
				persistence.OptionalWithDefault("image_id", &imageID),
			)
			if err != nil {
				return nil, err
			}

			imageIDs = append(imageIDs, imageID)
		}
	}

	return imageIDs, nil
}

func (s *storageYDB) imageBackupDeletionsCompleted(
	ctx context.Context,
	session *persistence.Session,
	imageIDs []string,
) error {

	if len(imageIDs) == 0 {
		return nil
	}

	var imageIDValues []persistence.Value
	for _, imageID := range imageIDs {
		imageIDValues = append(imageIDValues, persistence.UTF8Value(imageID))
	}

	_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_ids as List<Utf8>;

		delete from backup_delete_queue
		where image_id in $image_ids
	`, s.imagesPath),
		persistence.ValueParam(
			"$image_ids",
			persistence.ListValue(imageIDValues...),
		),
	)
	return err
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) CreateImage(
	ctx context.Context,
	image ImageMeta,
) (ImageMeta, error) {

	var created ImageMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			created, err = s.createImage(ctx, session, image)
			return err
		},
	)
	return created, err
}

func (s *storageYDB) ImageCreated(
	ctx context.Context,
	imageID string,
	checkpointID string,
	createdAt time.Time,
	imageSize uint64,
	imageStorageSize uint64,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.imageCreated(
				ctx,
				session,
				imageID,
				checkpointID,
				createdAt,
				imageSize,
				imageStorageSize,
			)
		},
	)
}

func (s *storageYDB) GetImageMeta(
	ctx context.Context,
	imageID string,
) (*ImageMeta, error) {

	var image *ImageMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			image, err = s.getImageMeta(ctx, session, imageID)
			return err
		},
	)
	return image, err
}

func (s *storageYDB) DeleteImage(
	ctx context.Context,
	imageID string,
	taskID string,
	deletingAt time.Time,
) (*ImageMeta, error) {

	var image *ImageMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			image, err = s.deleteImage(ctx, session, imageID, taskID, deletingAt)
			return err
		},
	)
	return image, err
}

func (s *storageYDB) ImageDeleted(
	ctx context.Context,
	imageID string,
	deletedAt time.Time,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.imageDeleted(ctx, session, imageID, deletedAt)
		},
	)
}

func (s *storageYDB) ClearDeletedImages(
	ctx context.Context,
	deletedBefore time.Time,
	limit int,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.clearDeletedImages(ctx, session, deletedBefore, limit)
		},
	)
}

func (s *storageYDB) ListImages(
	ctx context.Context,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	var ids []string

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			ids, err = s.listImages(ctx, session, folderID, creatingBefore)
			return err
		},
	)
	return ids, err
}

func (s *storageYDB) ListImagesToBackup(
	ctx context.Context,
	limit int,
) ([]ImageBackupRequest, error) {

	var backups []ImageBackupRequest
	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			backups, err = s.listImagesToBackup(ctx, session, limit)
			return err
		},
	)
	return backups, err
}

func (s *storageYDB) EnqueueImageBackup(
	ctx context.Context,
	imageID string,
	backupID string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.enqueueImageBackup(ctx, session, imageID, backupID)
		},
	)
}

func (s *storageYDB) ImageBackupScheduled(
	ctx context.Context,
	imageID string,
	backupID string,
	taskID string,
) (time.Time, error) {

	var enqueuedAt time.Time
	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			enqueuedAt, err = s.imageBackupScheduled(
				ctx,
				session,
				imageID,
				backupID,
				taskID,
			)
			return err
		},
	)
	return enqueuedAt, err
}

func (s *storageYDB) ListScheduledImageBackups(
	ctx context.Context,
	limit int,
) ([]ScheduledImageBackup, error) {

	var backups []ScheduledImageBackup
	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			backups, err = s.listScheduledImageBackups(ctx, session, limit)
			return err
		},
	)
	return backups, err
}

func (s *storageYDB) RemoveImageFromBackupQueue(
	ctx context.Context,
	imageID string,
	backupID string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.removeImageFromBackupQueue(ctx, session, imageID, backupID)
		},
	)
}

func (s *storageYDB) ImageBackupCompleted(
	ctx context.Context,
	imageID string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.imageBackupCompleted(ctx, session, imageID)
		},
	)
}

func (s *storageYDB) GetImageBackupQueueStats(
	ctx context.Context,
) (ImageBackupQueueStats, error) {

	var stats ImageBackupQueueStats
	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			stats, err = s.getImageBackupQueueStats(ctx, session)
			return err
		},
	)
	return stats, err
}

func (s *storageYDB) GetImageBackupDeleteQueue(
	ctx context.Context,
	limit int,
) ([]string, error) {

	var imageIDs []string

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			imageIDs, err = s.listImageIDsForDeletion(ctx, session, limit)
			return err
		},
	)
	return imageIDs, err
}

func (s *storageYDB) ImageBackupDeletionsCompleted(
	ctx context.Context,
	imageIDs []string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.imageBackupDeletionsCompleted(ctx, session, imageIDs)
		},
	)
}

////////////////////////////////////////////////////////////////////////////////

func createImagesYDBTables(
	ctx context.Context,
	folder string,
	db *persistence.YDBClient,
	dropUnusedColumns bool,
) error {

	logging.Info(ctx, "Creating tables for images in %v", db.AbsolutePath(folder))

	err := db.CreateOrAlterTable(
		ctx,
		folder,
		"images",
		imageStateTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created images table")

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		"deleted",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("deleted_at", persistence.Optional(persistence.TypeTimestamp)),
			persistence.WithColumn("image_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("deleted_at", "image_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created deleted table")

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		"backup_queue",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("image_id", persistence.Optional(persistence.TypeUTF8)),
			// Identifies the attempt: the create task of the image for one
			// queued by its folder, a new ID for one queued by hand.
			persistence.WithColumn(
				"backup_id",
				persistence.Optional(persistence.TypeUTF8),
			),
			// The images.BackupImage task of the attempt; empty while the
			// attempt waits for a free slot.
			persistence.WithColumn(
				"task_id",
				persistence.Optional(persistence.TypeUTF8),
			),
			persistence.WithColumn(
				"enqueued_at",
				persistence.Optional(persistence.TypeTimestamp),
			),
			persistence.WithPrimaryKeyColumn("image_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created backup_queue table")

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		"backup_delete_queue",
		persistence.NewCreateTableDescription(
			persistence.WithColumn(
				"image_id",
				persistence.Optional(persistence.TypeUTF8),
			),
			persistence.WithPrimaryKeyColumn("image_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created backup_delete_queue table")

	logging.Info(ctx, "Created tables for images")

	return nil
}

func dropImagesYDBTables(
	ctx context.Context,
	folder string,
	db *persistence.YDBClient,
) error {

	logging.Info(ctx, "Dropping tables for images in %v", db.AbsolutePath(folder))

	err := db.DropTable(ctx, folder, "images")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped images table")

	err = db.DropTable(ctx, folder, "deleted")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped deleted table")

	err = db.DropTable(ctx, folder, "backup_queue")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped backup_queue table")

	err = db.DropTable(ctx, folder, "backup_delete_queue")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped backup_delete_queue table")

	logging.Info(ctx, "Dropped tables for images")

	return nil
}
