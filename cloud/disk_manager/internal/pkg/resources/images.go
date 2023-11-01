package resources

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/persistence"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	ydb_result "github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	ydb_named "github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydb_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

////////////////////////////////////////////////////////////////////////////////

type imageStatus uint32

func (s *imageStatus) UnmarshalYDB(res ydb_types.RawValue) error {
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

	status imageStatus
}

func (s *imageState) toImageMeta() *ImageMeta {
	// TODO: Image.CreateRequest should be []byte, because we can't unmarshal
	// it here, without knowing particular protobuf message type.
	return &ImageMeta{
		ID:                s.id,
		FolderID:          s.folderID,
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
		Ready: s.status == imageStatusReady,
	}
}

func (s *imageState) structValue() ydb_types.Value {
	return ydb_types.StructValue(
		ydb_types.StructFieldValue("id", ydb_types.UTF8Value(s.id)),
		ydb_types.StructFieldValue("folder_id", ydb_types.UTF8Value(s.folderID)),
		ydb_types.StructFieldValue("create_request", ydb_types.StringValue(s.createRequest)),
		ydb_types.StructFieldValue("create_task_id", ydb_types.UTF8Value(s.createTaskID)),
		ydb_types.StructFieldValue("creating_at", persistence.TimestampValue(s.creatingAt)),
		ydb_types.StructFieldValue("created_at", persistence.TimestampValue(s.createdAt)),
		ydb_types.StructFieldValue("created_by", ydb_types.UTF8Value(s.createdBy)),
		ydb_types.StructFieldValue("delete_task_id", ydb_types.UTF8Value(s.deleteTaskID)),
		ydb_types.StructFieldValue("deleting_at", persistence.TimestampValue(s.deletingAt)),
		ydb_types.StructFieldValue("deleted_at", persistence.TimestampValue(s.deletedAt)),
		ydb_types.StructFieldValue("use_dataplane_tasks", ydb_types.BoolValue(s.useDataplaneTasks)),
		ydb_types.StructFieldValue("size", ydb_types.Uint64Value(s.size)),
		ydb_types.StructFieldValue("storage_size", ydb_types.Uint64Value(s.storageSize)),
		ydb_types.StructFieldValue("encryption_mode", ydb_types.Uint32Value(s.encryptionMode)),
		ydb_types.StructFieldValue("encryption_keyhash", ydb_types.StringValue(s.encryptionKeyHash)),
		ydb_types.StructFieldValue("status", ydb_types.Int64Value(int64(s.status))),
	)
}

func scanImageState(res ydb_result.Result) (state imageState, err error) {
	err = res.ScanNamed(
		ydb_named.OptionalWithDefault("id", &state.id),
		ydb_named.OptionalWithDefault("folder_id", &state.folderID),
		ydb_named.OptionalWithDefault("create_request", &state.createRequest),
		ydb_named.OptionalWithDefault("create_task_id", &state.createTaskID),
		ydb_named.OptionalWithDefault("creating_at", &state.creatingAt),
		ydb_named.OptionalWithDefault("created_at", &state.createdAt),
		ydb_named.OptionalWithDefault("created_by", &state.createdBy),
		ydb_named.OptionalWithDefault("delete_task_id", &state.deleteTaskID),
		ydb_named.OptionalWithDefault("deleting_at", &state.deletingAt),
		ydb_named.OptionalWithDefault("deleted_at", &state.deletedAt),
		ydb_named.OptionalWithDefault("use_dataplane_tasks", &state.useDataplaneTasks),
		ydb_named.OptionalWithDefault("status", &state.status),
		ydb_named.OptionalWithDefault("size", &state.size),
		ydb_named.OptionalWithDefault("storage_size", &state.storageSize),
		ydb_named.OptionalWithDefault("encryption_mode", &state.encryptionMode),
		ydb_named.OptionalWithDefault("encryption_keyhash", &state.encryptionKeyHash),
	)
	if err != nil {
		return state, errors.NewNonRetriableErrorf(
			"scanImageStates: failed to parse row: %w",
			err,
		)
	}

	return state, nil
}

func scanImageStates(
	ctx context.Context,
	res ydb_result.Result,
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
		create_request: String,
		create_task_id: Utf8,
		creating_at: Timestamp,
		created_at: Timestamp,
		created_by: Utf8,
		delete_task_id: Utf8,
		deleting_at: Timestamp,
		deleted_at: Timestamp,
		use_dataplane_tasks: Bool,
		size: Uint64,
		storage_size: Uint64,
		encryption_mode: Uint32,
		encryption_keyhash: String,
		status: Int64>`
}

func imageStateTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("id", ydb_types.Optional(ydb_types.TypeUTF8)),
		persistence.WithColumn("folder_id", ydb_types.Optional(ydb_types.TypeUTF8)),
		persistence.WithColumn("create_request", ydb_types.Optional(ydb_types.TypeString)),
		persistence.WithColumn("create_task_id", ydb_types.Optional(ydb_types.TypeUTF8)),
		persistence.WithColumn("creating_at", ydb_types.Optional(ydb_types.TypeTimestamp)),
		persistence.WithColumn("created_at", ydb_types.Optional(ydb_types.TypeTimestamp)),
		persistence.WithColumn("created_by", ydb_types.Optional(ydb_types.TypeUTF8)),
		persistence.WithColumn("delete_task_id", ydb_types.Optional(ydb_types.TypeUTF8)),
		persistence.WithColumn("deleting_at", ydb_types.Optional(ydb_types.TypeTimestamp)),
		persistence.WithColumn("deleted_at", ydb_types.Optional(ydb_types.TypeTimestamp)),
		persistence.WithColumn("use_dataplane_tasks", ydb_types.Optional(ydb_types.TypeBool)),
		persistence.WithColumn("size", ydb_types.Optional(ydb_types.TypeUint64)),
		persistence.WithColumn("storage_size", ydb_types.Optional(ydb_types.TypeUint64)),
		persistence.WithColumn("encryption_mode", ydb_types.Optional(ydb_types.TypeUint32)),
		persistence.WithColumn("encryption_keyhash", ydb_types.Optional(ydb_types.TypeString)),
		persistence.WithColumn("status", ydb_types.Optional(ydb_types.TypeInt64)),
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
		persistence.ValueParam("$id", ydb_types.UTF8Value(imageID)),
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
		commitErr := tx.Commit(ctx)
		if commitErr != nil {
			return false, commitErr
		}

		return false, errors.NewNonRetriableErrorf(
			"imageExists: failed to parse row: %w",
			err,
		)
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
		persistence.ValueParam("$id", ydb_types.UTF8Value(imageID)),
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
) (*ImageMeta, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback(ctx)

	// HACK: see NBS-974 for details.
	snapshotExists, err := s.snapshotExists(ctx, tx, image.ID)
	if err != nil {
		return nil, err
	}

	if snapshotExists {
		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}

		logging.Info(
			ctx,
			"image with id %v can't be created, because snapshot with id %v already exists",
			image.ID,
			image.ID,
		)
		return nil, nil
	}

	createRequest, err := proto.Marshal(image.CreateRequest)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
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
		persistence.ValueParam("$id", ydb_types.UTF8Value(image.ID)),
	)
	if err != nil {
		return nil, err
	}

	defer res.Close()

	states, err := scanImageStates(ctx, res)
	if err != nil {
		commitErr := tx.Commit(ctx)
		if commitErr != nil {
			return nil, commitErr
		}

		return nil, err
	}

	if len(states) != 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}

		state := states[0]

		if state.status >= imageStatusDeleting {
			logging.Info(ctx, "can't create already deleting/deleted image with id %v", image.ID)
			return nil, errors.NewSilentNonRetriableErrorf(
				"can't create already deleting/deleted image with id %v",
				image.ID,
			)
		}

		// Check idempotency.
		if bytes.Equal(state.createRequest, createRequest) &&
			state.createTaskID == image.CreateTaskID &&
			state.createdBy == image.CreatedBy {

			return state.toImageMeta(), nil
		}

		logging.Info(ctx, "image with different params already exists, old=%v, new=%v", state, image)
		return nil, nil
	}

	state := imageState{
		id:                image.ID,
		folderID:          image.FolderID,
		createRequest:     createRequest,
		createTaskID:      image.CreateTaskID,
		creatingAt:        image.CreatingAt,
		createdBy:         image.CreatedBy,
		useDataplaneTasks: image.UseDataplaneTasks,

		status: imageStatusCreating,
	}

	if image.Encryption != nil {
		state.encryptionMode = uint32(image.Encryption.Mode)

		switch key := image.Encryption.Key.(type) {
		case *types.EncryptionDesc_KeyHash:
			state.encryptionKeyHash = key.KeyHash
		case nil:
			state.encryptionKeyHash = nil
		default:
			return nil, errors.NewNonRetriableErrorf("unknown key %s", key)
		}
	} else {
		state.encryptionMode = uint32(types.EncryptionMode_NO_ENCRYPTION)
		state.encryptionKeyHash = nil
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into images
		select *
		from AS_TABLE($states)
	`, s.imagesPath, imageStateStructTypeString()),
		persistence.ValueParam("$states", ydb_types.ListValue(state.structValue())),
	)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return state.toImageMeta(), nil
}

func (s *storageYDB) imageCreated(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
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
		persistence.ValueParam("$id", ydb_types.UTF8Value(imageID)),
	)
	if err != nil {
		return err
	}

	defer res.Close()

	states, err := scanImageStates(ctx, res)
	if err != nil {
		commitErr := tx.Commit(ctx)
		if commitErr != nil {
			return commitErr
		}

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
		persistence.ValueParam("$states", ydb_types.ListValue(state.structValue())),
	)
	if err != nil {
		return err
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

		logging.Info(
			ctx,
			"image with id %v can't be deleted, because snapshot with id %v already exists",
			imageID,
			imageID,
		)
		return nil, nil
	}

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from images
		where id = $id
	`, s.imagesPath),
		persistence.ValueParam("$id", ydb_types.UTF8Value(imageID)),
	)
	if err != nil {
		return nil, err
	}

	defer res.Close()

	states, err := scanImageStates(ctx, res)
	if err != nil {
		commitErr := tx.Commit(ctx)
		if commitErr != nil {
			return nil, commitErr
		}

		return nil, err
	}

	var state imageState

	if len(states) != 0 {
		state = states[0]

		if state.status >= imageStatusDeleting {
			// Image already marked as deleting/deleted.

			err = tx.Commit(ctx)
			if err != nil {
				return nil, err
			}

			return state.toImageMeta(), nil
		}
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
		persistence.ValueParam("$states", ydb_types.ListValue(state.structValue())),
	)
	if err != nil {
		return nil, err
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
		persistence.ValueParam("$id", ydb_types.UTF8Value(imageID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanImageStates(ctx, res)
	if err != nil {
		commitErr := tx.Commit(ctx)
		if commitErr != nil {
			return commitErr
		}

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
		persistence.ValueParam("$states", ydb_types.ListValue(state.structValue())),
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
		persistence.ValueParam("$image_id", ydb_types.UTF8Value(imageID)),
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
		persistence.ValueParam("$limit", ydb_types.Uint64Value(uint64(limit))),
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
				ydb_named.OptionalWithDefault("deleted_at", &deletedAt),
				ydb_named.OptionalWithDefault("image_id", &imageID),
			)
			if err != nil {
				return errors.NewNonRetriableErrorf(
					"clearDeletedImages: failed to parse row: %w",
					err,
				)
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
				persistence.ValueParam("$image_id", ydb_types.UTF8Value(imageID)),
				persistence.ValueParam("$status", ydb_types.Int64Value(int64(imageStatusDeleted))),
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

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) CreateImage(
	ctx context.Context,
	image ImageMeta,
) (*ImageMeta, error) {

	var created *ImageMeta

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
			persistence.WithColumn("deleted_at", ydb_types.Optional(ydb_types.TypeTimestamp)),
			persistence.WithColumn("image_id", ydb_types.Optional(ydb_types.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("deleted_at", "image_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created deleted table")

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

	logging.Info(ctx, "Dropped tables for images")

	return nil
}
