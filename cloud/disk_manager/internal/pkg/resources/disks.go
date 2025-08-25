package resources

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type diskStatus uint32

func (s *diskStatus) UnmarshalYDB(res persistence.RawValue) error {
	*s = diskStatus(res.Int64())
	return nil
}

// NOTE: These values are stored in DB, do not shuffle them around.
const (
	diskStatusCreating diskStatus = iota
	diskStatusReady    diskStatus = iota
	diskStatusDeleting diskStatus = iota
	diskStatusDeleted  diskStatus = iota
)

func diskStatusToString(status diskStatus) string {
	switch status {
	case diskStatusCreating:
		return "creating"
	case diskStatusReady:
		return "ready"
	case diskStatusDeleting:
		return "deleting"
	case diskStatusDeleted:
		return "deleted"
	}

	return fmt.Sprintf("unknown_%v", status)
}

////////////////////////////////////////////////////////////////////////////////

// This is mapped into a DB row. If you change this struct, make sure to update
// the mapping code.
type diskState struct {
	id               string
	zoneID           string
	srcImageID       string
	srcSnapshotID    string
	blocksCount      uint64
	blockSize        uint32
	kind             string
	cloudID          string
	folderID         string
	placementGroupID string

	baseDiskID           string
	baseDiskCheckpointID string

	createRequest []byte
	createTaskID  string
	creatingAt    time.Time
	createdAt     time.Time
	createdBy     string
	deleteTaskID  string
	deletingAt    time.Time
	deletedAt     time.Time

	status diskStatus

	scannedAt            time.Time
	scanFoundBrokenBlobs bool

	fillGeneration uint64
}

func (s *diskState) toDiskMeta() *DiskMeta {
	// TODO: Disk.CreateRequest should be []byte, because we can't unmarshal
	// it here, without knowing particular protobuf message type.
	return &DiskMeta{
		ID:               s.id,
		ZoneID:           s.zoneID,
		SrcImageID:       s.srcImageID,
		SrcSnapshotID:    s.srcSnapshotID,
		BlocksCount:      s.blocksCount,
		BlockSize:        s.blockSize,
		Kind:             s.kind,
		CloudID:          s.cloudID,
		FolderID:         s.folderID,
		PlacementGroupID: s.placementGroupID,

		CreateTaskID: s.createTaskID,
		CreatingAt:   s.creatingAt,
		CreatedAt:    s.createdAt,
		CreatedBy:    s.createdBy,
		DeleteTaskID: s.deleteTaskID,

		ScannedAt:            s.scannedAt,
		ScanFoundBrokenBlobs: s.scanFoundBrokenBlobs,

		FillGeneration: s.fillGeneration,
	}
}

func (s *diskState) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("id", persistence.UTF8Value(s.id)),
		persistence.StructFieldValue("zone_id", persistence.UTF8Value(s.zoneID)),
		persistence.StructFieldValue("src_image_id", persistence.UTF8Value(s.srcImageID)),
		persistence.StructFieldValue("src_snapshot_id", persistence.UTF8Value(s.srcSnapshotID)),
		persistence.StructFieldValue("blocks_count", persistence.Uint64Value(s.blocksCount)),
		persistence.StructFieldValue("block_size", persistence.Uint32Value(s.blockSize)),
		persistence.StructFieldValue("kind", persistence.UTF8Value(s.kind)),
		persistence.StructFieldValue("cloud_id", persistence.UTF8Value(s.cloudID)),
		persistence.StructFieldValue("folder_id", persistence.UTF8Value(s.folderID)),
		persistence.StructFieldValue("placement_group_id", persistence.UTF8Value(s.placementGroupID)),

		persistence.StructFieldValue("base_disk_id", persistence.UTF8Value(s.baseDiskID)),
		persistence.StructFieldValue("base_disk_checkpoint_id", persistence.UTF8Value(s.baseDiskCheckpointID)),

		persistence.StructFieldValue("create_request", persistence.StringValue(s.createRequest)),
		persistence.StructFieldValue("create_task_id", persistence.UTF8Value(s.createTaskID)),
		persistence.StructFieldValue("creating_at", persistence.TimestampValue(s.creatingAt)),
		persistence.StructFieldValue("created_at", persistence.TimestampValue(s.createdAt)),
		persistence.StructFieldValue("created_by", persistence.UTF8Value(s.createdBy)),
		persistence.StructFieldValue("delete_task_id", persistence.UTF8Value(s.deleteTaskID)),
		persistence.StructFieldValue("deleting_at", persistence.TimestampValue(s.deletingAt)),
		persistence.StructFieldValue("deleted_at", persistence.TimestampValue(s.deletedAt)),

		persistence.StructFieldValue("status", persistence.Int64Value(int64(s.status))),

		persistence.StructFieldValue("scanned_at", persistence.TimestampValue(s.scannedAt)),
		persistence.StructFieldValue("scan_found_broken_blobs", persistence.BoolValue(s.scanFoundBrokenBlobs)),

		persistence.StructFieldValue("fill_generation", persistence.Uint64Value(s.fillGeneration)),
	)
}

func scanDiskState(res persistence.Result) (state diskState, err error) {
	err = res.ScanNamed(
		persistence.OptionalWithDefault("id", &state.id),
		persistence.OptionalWithDefault("zone_id", &state.zoneID),
		persistence.OptionalWithDefault("src_image_id", &state.srcImageID),
		persistence.OptionalWithDefault("src_snapshot_id", &state.srcSnapshotID),
		persistence.OptionalWithDefault("blocks_count", &state.blocksCount),
		persistence.OptionalWithDefault("block_size", &state.blockSize),
		persistence.OptionalWithDefault("kind", &state.kind),
		persistence.OptionalWithDefault("cloud_id", &state.cloudID),
		persistence.OptionalWithDefault("folder_id", &state.folderID),
		persistence.OptionalWithDefault("placement_group_id", &state.placementGroupID),
		persistence.OptionalWithDefault("base_disk_id", &state.baseDiskID),
		persistence.OptionalWithDefault("base_disk_checkpoint_id", &state.baseDiskCheckpointID),
		persistence.OptionalWithDefault("create_request", &state.createRequest),
		persistence.OptionalWithDefault("create_task_id", &state.createTaskID),
		persistence.OptionalWithDefault("creating_at", &state.creatingAt),
		persistence.OptionalWithDefault("created_at", &state.createdAt),
		persistence.OptionalWithDefault("created_by", &state.createdBy),
		persistence.OptionalWithDefault("delete_task_id", &state.deleteTaskID),
		persistence.OptionalWithDefault("deleting_at", &state.deletingAt),
		persistence.OptionalWithDefault("deleted_at", &state.deletedAt),
		persistence.OptionalWithDefault("status", &state.status),
		persistence.OptionalWithDefault("scanned_at", &state.scannedAt),
		persistence.OptionalWithDefault("scan_found_broken_blobs", &state.scanFoundBrokenBlobs),
		persistence.OptionalWithDefault("fill_generation", &state.fillGeneration),
	)
	return
}

func scanDiskStates(
	ctx context.Context,
	res persistence.Result,
) ([]diskState, error) {

	var states []diskState
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			state, err := scanDiskState(res)
			if err != nil {
				return nil, err
			}

			states = append(states, state)
		}
	}

	return states, nil
}

func diskStateStructTypeString() string {
	return `Struct<
		id: Utf8,
		zone_id: Utf8,
		src_image_id: Utf8,
		src_snapshot_id: Utf8,
		blocks_count: Uint64,
		block_size: Uint32,
		kind: Utf8,
		cloud_id: Utf8,
		folder_id: Utf8,
		placement_group_id: Utf8,

		base_disk_id: Utf8,
		base_disk_checkpoint_id: Utf8,

		create_request: String,
		create_task_id: Utf8,
		creating_at: Timestamp,
		created_at: Timestamp,
		created_by: Utf8,
		delete_task_id: Utf8,
		deleting_at: Timestamp,
		deleted_at: Timestamp,
		status: Int64,

		scanned_at: Timestamp,
		scan_found_broken_blobs: Bool,

		fill_generation: Uint64>`
}

func diskStateTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("src_image_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("src_snapshot_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("blocks_count", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("block_size", persistence.Optional(persistence.TypeUint32)),
		persistence.WithColumn("kind", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("cloud_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("folder_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("placement_group_id", persistence.Optional(persistence.TypeUTF8)),

		persistence.WithColumn("base_disk_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("base_disk_checkpoint_id", persistence.Optional(persistence.TypeUTF8)),

		persistence.WithColumn("create_request", persistence.Optional(persistence.TypeString)),
		persistence.WithColumn("create_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("creating_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("created_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("created_by", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("delete_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("deleting_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("deleted_at", persistence.Optional(persistence.TypeTimestamp)),

		persistence.WithColumn("status", persistence.Optional(persistence.TypeInt64)),

		persistence.WithColumn("scanned_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("scan_found_broken_blobs", persistence.Optional(persistence.TypeBool)),

		persistence.WithColumn("fill_generation", persistence.Optional(persistence.TypeUint64)),

		persistence.WithPrimaryKeyColumn("id"),
	)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) getDiskMeta(
	ctx context.Context,
	session *persistence.Session,
	diskID string,
) (*DiskMeta, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from disks
		where id = $id
	`, s.disksPath),
		persistence.ValueParam("$id", persistence.UTF8Value(diskID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	states, err := scanDiskStates(ctx, res)
	if err != nil {
		return nil, err
	}

	if len(states) != 0 {
		return states[0].toDiskMeta(), nil
	} else {
		return nil, nil
	}
}

func (s *storageYDB) createDisk(
	ctx context.Context,
	session *persistence.Session,
	disk DiskMeta,
) (*DiskMeta, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	createRequest, err := proto.Marshal(disk.CreateRequest)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to marshal create request for disk with id %v: %w",
			disk.ID,
			err,
		)
	}

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from disks
		where id = $id
	`, s.disksPath),
		persistence.ValueParam("$id", persistence.UTF8Value(disk.ID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	states, err := scanDiskStates(ctx, res)
	if err != nil {
		return nil, err
	}

	if len(states) != 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}

		state := states[0]

		if state.status >= diskStatusDeleting {
			logging.Info(ctx, "can't create already deleting/deleted disk with id %v", disk.ID)
			return nil, errors.NewSilentNonRetriableErrorf(
				"can't create already deleting/deleted disk with id %v",
				disk.ID,
			)
		}

		// Check idempotency.
		if bytes.Equal(state.createRequest, createRequest) &&
			state.createTaskID == disk.CreateTaskID &&
			state.createdBy == disk.CreatedBy {

			return state.toDiskMeta(), nil
		}

		logging.Info(ctx, "disk with different params already exists, old=%v, new=%v", state, disk)
		return nil, nil
	}

	state := diskState{
		id:               disk.ID,
		zoneID:           disk.ZoneID,
		srcImageID:       disk.SrcImageID,
		srcSnapshotID:    disk.SrcSnapshotID,
		blocksCount:      disk.BlocksCount,
		blockSize:        disk.BlockSize,
		kind:             disk.Kind,
		cloudID:          disk.CloudID,
		folderID:         disk.FolderID,
		placementGroupID: disk.PlacementGroupID,

		createRequest: createRequest,
		createTaskID:  disk.CreateTaskID,
		creatingAt:    disk.CreatingAt,
		createdBy:     disk.CreatedBy,

		status: diskStatusCreating,
	}

	err = s.updateDiskState(ctx, tx, state)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return state.toDiskMeta(), nil
}

func (s *storageYDB) diskCreated(
	ctx context.Context,
	session *persistence.Session,
	disk DiskMeta,
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
		from disks
		where id = $id
	`, s.disksPath),
		persistence.ValueParam("$id", persistence.UTF8Value(disk.ID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanDiskStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"disk with id %v is not found",
			disk.ID,
		)
	}

	state := states[0]

	if state.status == diskStatusReady {
		// Nothing to do.
		return tx.Commit(ctx)
	}

	if state.status != diskStatusCreating {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"disk with id %v and status %v can't be created",
			disk.ID,
			diskStatusToString(state.status),
		)
	}

	state.status = diskStatusReady
	state.createdAt = disk.CreatedAt

	err = s.updateDiskState(ctx, tx, state)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) deleteDisk(
	ctx context.Context,
	session *persistence.Session,
	diskID string,
	taskID string,
	deletingAt time.Time,
) (*DiskMeta, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from disks
		where id = $id
	`, s.disksPath),
		persistence.ValueParam("$id", persistence.UTF8Value(diskID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	states, err := scanDiskStates(ctx, res)
	if err != nil {
		return nil, err
	}

	var state diskState

	if len(states) != 0 {
		state = states[0]

		if state.status >= diskStatusDeleting {
			// Disk already marked as deleting/deleted.

			err = tx.Commit(ctx)
			if err != nil {
				return nil, err
			}

			return state.toDiskMeta(), nil
		}

		state.status = diskStatusDeleting
	} else {
		state.status = diskStatusDeleted
	}

	state.id = diskID
	state.deleteTaskID = taskID
	state.deletingAt = deletingAt

	err = s.updateDiskState(ctx, tx, state)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return state.toDiskMeta(), nil
}

func (s *storageYDB) diskDeleted(
	ctx context.Context,
	session *persistence.Session,
	diskID string,
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
		from disks
		where id = $id
	`, s.disksPath),
		persistence.ValueParam("$id", persistence.UTF8Value(diskID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanDiskStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		// It's possible that disk is already collected.
		return tx.Commit(ctx)
	}

	state := states[0]

	if state.status == diskStatusDeleted {
		// Nothing to do.
		return tx.Commit(ctx)
	}

	if state.status != diskStatusDeleting {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"disk with id %v and status %v can't be deleted",
			diskID,
			diskStatusToString(state.status),
		)
	}

	state.status = diskStatusDeleted
	state.deletedAt = deletedAt

	err = s.updateDiskState(ctx, tx, state)
	if err != nil {
		return err
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $deleted_at as Timestamp;
		declare $disk_id as Utf8;

		upsert into deleted (deleted_at, disk_id)
		values ($deleted_at, $disk_id)
	`, s.disksPath),
		persistence.ValueParam("$deleted_at", persistence.TimestampValue(deletedAt)),
		persistence.ValueParam("$disk_id", persistence.UTF8Value(diskID)),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) clearDeletedDisks(
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
	`, s.disksPath),
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
				diskID    string
			)
			err = res.ScanNamed(
				persistence.OptionalWithDefault("deleted_at", &deletedAt),
				persistence.OptionalWithDefault("disk_id", &diskID),
			)
			if err != nil {
				return err
			}

			_, err = session.ExecuteRW(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				declare $deleted_at as Timestamp;
				declare $disk_id as Utf8;
				declare $status as Int64;

				delete from disks
				where id = $disk_id and status = $status;

				delete from deleted
				where deleted_at = $deleted_at and disk_id = $disk_id
			`, s.disksPath),
				persistence.ValueParam("$deleted_at", persistence.TimestampValue(deletedAt)),
				persistence.ValueParam("$disk_id", persistence.UTF8Value(diskID)),
				persistence.ValueParam("$status", persistence.Int64Value(int64(diskStatusDeleted))),
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *storageYDB) listDisks(
	ctx context.Context,
	session *persistence.Session,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	return listResources(
		ctx,
		session,
		s.disksPath,
		"disks",
		folderID,
		creatingBefore,
	)
}

func (s *storageYDB) incrementFillGeneration(
	ctx context.Context,
	session *persistence.Session,
	diskID string,
) (uint64, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from disks
		where id = $id
	`, s.disksPath),
		persistence.ValueParam("$id", persistence.UTF8Value(diskID)),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	states, err := scanDiskStates(ctx, res)
	if err != nil {
		return 0, err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return 0, err
		}

		return 0, errors.NewSilentNonRetriableErrorf(
			"unable to find disk %q",
			diskID,
		)
	}

	state := states[0]

	state.fillGeneration++

	err = s.updateDiskState(ctx, tx, state)
	if err != nil {
		return 0, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return 0, err
	}

	return state.fillGeneration, nil
}

func (s *storageYDB) diskScanned(
	ctx context.Context,
	session *persistence.Session,
	diskID string,
	scannedAt time.Time,
	foundBrokenBlobs bool,
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
		from disks
		where id = $id
	`, s.disksPath),
		persistence.ValueParam("$id", persistence.UTF8Value(diskID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanDiskStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"disk with id %v is not found",
			diskID,
		)
	}

	state := states[0]

	if state.status != diskStatusReady {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"disk with id %v and status %v can't be scanned",
			diskID,
			diskStatusToString(state.status),
		)
	}

	state.scannedAt = scannedAt
	state.scanFoundBrokenBlobs = foundBrokenBlobs

	err = s.updateDiskState(ctx, tx, state)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) getDiskState(
	ctx context.Context,
	tx *persistence.Transaction,
	diskID string,
) (state diskState, err error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from disks
		where id = $id
	`, s.disksPath),
		persistence.ValueParam("$id", persistence.UTF8Value(diskID)),
	)
	if err != nil {
		return state, err
	}
	defer res.Close()

	states, err := scanDiskStates(ctx, res)
	if err != nil {
		return state, err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return state, err
		}

		return state, errors.NewSilentNonRetriableErrorf(
			"unable to find disk %q",
			diskID,
		)
	}
	return states[0], nil
}

func (s *storageYDB) updateDiskState(
	ctx context.Context,
	tx *persistence.Transaction,
	state diskState,
) error {

	_, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into disks
		select *
		from AS_TABLE($states)
	`, s.disksPath, diskStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
	)
	return err
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) CreateDisk(
	ctx context.Context,
	disk DiskMeta,
) (*DiskMeta, error) {

	var created *DiskMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			created, err = s.createDisk(ctx, session, disk)
			return err
		},
	)
	return created, err
}

func (s *storageYDB) DiskCreated(ctx context.Context, disk DiskMeta) error {
	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.diskCreated(ctx, session, disk)
		},
	)
}

func (s *storageYDB) GetDiskMeta(
	ctx context.Context,
	diskID string,
) (*DiskMeta, error) {

	var disk *DiskMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			disk, err = s.getDiskMeta(ctx, session, diskID)
			return err
		},
	)
	return disk, err
}

func (s *storageYDB) DeleteDisk(
	ctx context.Context,
	diskID string,
	taskID string,
	deletingAt time.Time,
) (*DiskMeta, error) {

	var disk *DiskMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			disk, err = s.deleteDisk(ctx, session, diskID, taskID, deletingAt)
			return err
		},
	)
	return disk, err
}

func (s *storageYDB) DiskDeleted(
	ctx context.Context,
	diskID string,
	deletedAt time.Time,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.diskDeleted(ctx, session, diskID, deletedAt)
		},
	)
}

func (s *storageYDB) ClearDeletedDisks(
	ctx context.Context,
	deletedBefore time.Time,
	limit int,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.clearDeletedDisks(ctx, session, deletedBefore, limit)
		},
	)
}

func (s *storageYDB) ListDisks(
	ctx context.Context,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	var ids []string

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			ids, err = s.listDisks(ctx, session, folderID, creatingBefore)
			return err
		},
	)
	return ids, err
}

func (s *storageYDB) IncrementFillGeneration(
	ctx context.Context,
	diskID string,
) (uint64, error) {

	var fillGeneration uint64

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			fillGeneration, err = s.incrementFillGeneration(
				ctx,
				session,
				diskID,
			)
			return err
		},
	)

	return fillGeneration, err
}

func (s *storageYDB) DiskScanned(
	ctx context.Context,
	diskID string,
	scannedAt time.Time,
	foundBrokenBlobs bool,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.diskScanned(
				ctx,
				session,
				diskID,
				scannedAt,
				foundBrokenBlobs,
			)
		},
	)
}

func (s *storageYDB) DiskRelocated(
	ctx context.Context,
	tx *persistence.Transaction,
	diskID string,
	srcZoneID string,
	dstZoneID string,
) error {

	state, err := s.getDiskState(ctx, tx, diskID)
	if err != nil {
		return err
	}

	oldZoneID := state.zoneID
	if oldZoneID != srcZoneID {
		return errors.NewNonRetriableErrorf(
			"Disk has been already relocated by another competing task.",
		)
	}

	state.zoneID = dstZoneID

	return s.updateDiskState(ctx, tx, state)
}

////////////////////////////////////////////////////////////////////////////////

func createDisksYDBTables(
	ctx context.Context,
	folder string,
	db *persistence.YDBClient,
	dropUnusedColumns bool,
) error {

	logging.Info(ctx, "Creating tables for disks in %v", db.AbsolutePath(folder))

	err := db.CreateOrAlterTable(
		ctx,
		folder,
		"disks",
		diskStateTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created disks table")

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		"deleted",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("deleted_at", persistence.Optional(persistence.TypeTimestamp)),
			persistence.WithColumn("disk_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("deleted_at", "disk_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created deleted table")

	logging.Info(ctx, "Created tables for disks")

	return nil
}

func dropDisksYDBTables(
	ctx context.Context,
	folder string,
	db *persistence.YDBClient,
) error {

	logging.Info(ctx, "Dropping tables for disks in %v", db.AbsolutePath(folder))

	err := db.DropTable(ctx, folder, "disks")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped disks table")

	err = db.DropTable(ctx, folder, "deleted")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped deleted table")

	logging.Info(ctx, "Dropped tables for disks")

	return nil
}
