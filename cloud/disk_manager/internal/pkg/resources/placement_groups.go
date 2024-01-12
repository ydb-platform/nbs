package resources

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/persistence"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

type placementGroupStatus uint32

func (s *placementGroupStatus) UnmarshalYDB(res persistence.RawValue) error {
	*s = placementGroupStatus(res.Int64())
	return nil
}

// NOTE: These values are stored in DB, do not shuffle them around.
const (
	placementGroupStatusCreating placementGroupStatus = iota
	placementGroupStatusReady    placementGroupStatus = iota
	placementGroupStatusDeleting placementGroupStatus = iota
	placementGroupStatusDeleted  placementGroupStatus = iota
)

func placementGroupStatusToString(status placementGroupStatus) string {
	switch status {
	case placementGroupStatusCreating:
		return "creating"
	case placementGroupStatusReady:
		return "ready"
	case placementGroupStatusDeleting:
		return "deleting"
	case placementGroupStatusDeleted:
		return "deleted"
	}

	return fmt.Sprintf("unknown_%v", status)
}

////////////////////////////////////////////////////////////////////////////////

// This is mapped into a DB row. If you change this struct, make sure to update
// the mapping code.
type placementGroupState struct {
	id            string
	zoneID        string
	createRequest []byte
	createTaskID  string
	creatingAt    time.Time
	createdAt     time.Time
	createdBy     string
	deleteTaskID  string
	deletingAt    time.Time
	deletedAt     time.Time

	placementStrategy       types.PlacementStrategy
	placementPartitionCount uint32

	status placementGroupStatus
}

func (s *placementGroupState) toPlacementGroupMeta() *PlacementGroupMeta {
	return &PlacementGroupMeta{
		ID:                      s.id,
		ZoneID:                  s.zoneID,
		PlacementStrategy:       s.placementStrategy,
		PlacementPartitionCount: s.placementPartitionCount,
		CreateTaskID:            s.createTaskID,
		CreatingAt:              s.creatingAt,
		CreatedAt:               s.createdAt,
		CreatedBy:               s.createdBy,
		DeleteTaskID:            s.deleteTaskID,
	}
}

func (s *placementGroupState) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("id", persistence.UTF8Value(s.id)),
		persistence.StructFieldValue("zone_id", persistence.UTF8Value(s.zoneID)),
		persistence.StructFieldValue(
			"placement_strategy",
			persistence.Int32Value(int32(s.placementStrategy)),
		),
		persistence.StructFieldValue(
			"placement_partition_count",
			persistence.Uint32Value(s.placementPartitionCount),
		),
		persistence.StructFieldValue(
			"create_request",
			persistence.StringValue(s.createRequest),
		),
		persistence.StructFieldValue(
			"create_task_id",
			persistence.UTF8Value(s.createTaskID),
		),
		persistence.StructFieldValue(
			"creating_at",
			persistence.TimestampValue(s.creatingAt),
		),
		persistence.StructFieldValue(
			"created_at",
			persistence.TimestampValue(s.createdAt),
		),
		persistence.StructFieldValue("created_by", persistence.UTF8Value(s.createdBy)),
		persistence.StructFieldValue(
			"delete_task_id",
			persistence.UTF8Value(s.deleteTaskID),
		),
		persistence.StructFieldValue(
			"deleting_at",
			persistence.TimestampValue(s.deletingAt),
		),
		persistence.StructFieldValue(
			"deleted_at",
			persistence.TimestampValue(s.deletedAt),
		),
		persistence.StructFieldValue(
			"status",
			persistence.Int64Value(int64(s.status)),
		),
	)
}

func scanPlacementGroupState(res persistence.Result) (state placementGroupState, err error) {
	var placementStrategy int32
	err = res.ScanNamed(
		persistence.OptionalWithDefault("id", &state.id),
		persistence.OptionalWithDefault("zone_id", &state.zoneID),
		persistence.OptionalWithDefault("placement_strategy", &placementStrategy),
		persistence.OptionalWithDefault("placement_partition_count", &state.placementPartitionCount),
		persistence.OptionalWithDefault("create_request", &state.createRequest),
		persistence.OptionalWithDefault("create_task_id", &state.createTaskID),
		persistence.OptionalWithDefault("creating_at", &state.creatingAt),
		persistence.OptionalWithDefault("created_at", &state.createdAt),
		persistence.OptionalWithDefault("created_by", &state.createdBy),
		persistence.OptionalWithDefault("delete_task_id", &state.deleteTaskID),
		persistence.OptionalWithDefault("deleting_at", &state.deletingAt),
		persistence.OptionalWithDefault("deleted_at", &state.deletedAt),
		persistence.OptionalWithDefault("status", &state.status),
	)
	if err != nil {
		return
	}

	state.placementStrategy = types.PlacementStrategy(placementStrategy)
	return
}

func scanPlacementGroupStates(
	ctx context.Context,
	res persistence.Result,
) ([]placementGroupState, error) {

	var states []placementGroupState
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			state, err := scanPlacementGroupState(res)
			if err != nil {
				return nil, err
			}

			states = append(states, state)
		}
	}

	return states, nil
}

func placementGroupStateStructTypeString() string {
	return `Struct<
		id: Utf8,
		zone_id: Utf8,
		placement_strategy: Int32,
		placement_partition_count: Uint32,
		create_request: String,
		create_task_id: Utf8,
		creating_at: Timestamp,
		created_at: Timestamp,
		created_by: Utf8,
		delete_task_id: Utf8,
		deleting_at: Timestamp,
		deleted_at: Timestamp,
		status: Int64>`
}

func placementGroupStateTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn(
			"placement_strategy",
			persistence.Optional(persistence.TypeInt32),
		),
		persistence.WithColumn(
			"placement_partition_count",
			persistence.Optional(persistence.TypeUint32),
		),
		persistence.WithColumn(
			"create_request",
			persistence.Optional(persistence.TypeString),
		),
		persistence.WithColumn(
			"create_task_id",
			persistence.Optional(persistence.TypeUTF8),
		),
		persistence.WithColumn(
			"creating_at",
			persistence.Optional(persistence.TypeTimestamp),
		),
		persistence.WithColumn(
			"created_at",
			persistence.Optional(persistence.TypeTimestamp),
		),
		persistence.WithColumn(
			"created_by",
			persistence.Optional(persistence.TypeUTF8),
		),
		persistence.WithColumn(
			"delete_task_id",
			persistence.Optional(persistence.TypeUTF8),
		),
		persistence.WithColumn(
			"deleting_at",
			persistence.Optional(persistence.TypeTimestamp),
		),
		persistence.WithColumn(
			"deleted_at",
			persistence.Optional(persistence.TypeTimestamp),
		),
		persistence.WithColumn(
			"status",
			persistence.Optional(persistence.TypeInt64),
		),
		persistence.WithPrimaryKeyColumn("id"),
	)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) getPlacementGroupState(
	ctx context.Context,
	session *persistence.Session,
	placementGroupID string,
) (*placementGroupState, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from placement_groups
		where id = $id
	`, s.placementGroupsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(placementGroupID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	states, err := scanPlacementGroupStates(ctx, res)
	if err != nil {
		return nil, err
	}

	if len(states) != 0 {
		return &states[0], nil
	} else {
		return nil, nil
	}
}

func (s *storageYDB) getPlacementGroupMeta(
	ctx context.Context,
	session *persistence.Session,
	placementGroupID string,
) (*PlacementGroupMeta, error) {

	state, err := s.getPlacementGroupState(ctx, session, placementGroupID)
	if err != nil {
		return nil, err
	}

	if state == nil {
		return nil, nil
	}

	return state.toPlacementGroupMeta(), nil
}

func (s *storageYDB) checkPlacementGroupReady(
	ctx context.Context,
	session *persistence.Session,
	placementGroupID string,
) (*PlacementGroupMeta, error) {

	state, err := s.getPlacementGroupState(ctx, session, placementGroupID)
	if err != nil {
		return nil, err
	}

	if state == nil {
		return nil, errors.NewNonRetriableErrorf(
			"placementGroup with id %v is not found",
			placementGroupID,
		)
	}

	if state.status != placementGroupStatusReady {
		return nil, errors.NewNonRetriableErrorf(
			"placementGroup with id %v is not ready",
			placementGroupID,
		)
	}

	return state.toPlacementGroupMeta(), nil
}

func (s *storageYDB) createPlacementGroup(
	ctx context.Context,
	session *persistence.Session,
	placementGroup PlacementGroupMeta,
) (*PlacementGroupMeta, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback(ctx)

	createRequest, err := proto.Marshal(placementGroup.CreateRequest)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to marshal create request for placementGroup with id %v: %w",
			placementGroup.ID,
			err,
		)
	}

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from placement_groups
		where id = $id
	`, s.placementGroupsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(placementGroup.ID)),
	)
	if err != nil {
		return nil, err
	}

	defer res.Close()

	states, err := scanPlacementGroupStates(ctx, res)
	if err != nil {
		return nil, err
	}

	if len(states) != 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return nil, err
		}

		state := states[0]

		if state.status >= placementGroupStatusDeleting {
			logging.Info(
				ctx,
				"can't create already deleting/deleted placementGroup with id %v",
				placementGroup.ID,
			)
			return nil, errors.NewSilentNonRetriableErrorf(
				"can't create already deleting/deleted placementGroup with id %v",
				placementGroup.ID,
			)
		}

		// Check idempotency.
		if bytes.Equal(state.createRequest, createRequest) &&
			state.createTaskID == placementGroup.CreateTaskID &&
			state.createdBy == placementGroup.CreatedBy {

			return state.toPlacementGroupMeta(), nil
		}

		logging.Info(
			ctx,
			"placementGroup with different params already exists, old=%v, new=%v",
			state,
			placementGroup,
		)
		return nil, nil
	}

	state := placementGroupState{
		id:                      placementGroup.ID,
		zoneID:                  placementGroup.ZoneID,
		placementStrategy:       placementGroup.PlacementStrategy,
		placementPartitionCount: placementGroup.PlacementPartitionCount,
		createRequest:           createRequest,
		createTaskID:            placementGroup.CreateTaskID,
		creatingAt:              placementGroup.CreatingAt,
		createdBy:               placementGroup.CreatedBy,

		status: placementGroupStatusCreating,
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into placement_groups
		select *
		from AS_TABLE($states)
	`,
		s.placementGroupsPath,
		placementGroupStateStructTypeString()),
		persistence.ValueParam(
			"$states",
			persistence.ListValue(state.structValue()),
		),
	)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return state.toPlacementGroupMeta(), nil
}

func (s *storageYDB) placementGroupCreated(
	ctx context.Context,
	session *persistence.Session,
	placementGroup PlacementGroupMeta,
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
		from placement_groups
		where id = $id
	`, s.placementGroupsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(placementGroup.ID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanPlacementGroupStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"placementGroup with id %v is not found",
			placementGroup.ID,
		)
	}

	state := states[0]

	if state.status == placementGroupStatusReady {
		// Nothing to do.
		return tx.Commit(ctx)
	}

	if state.status != placementGroupStatusCreating {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"placementGroup with id %v and status %v can't be created",
			placementGroup.ID,
			placementGroupStatusToString(state.status),
		)
	}

	state.status = placementGroupStatusReady
	state.createdAt = placementGroup.CreatedAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into placement_groups
		select *
		from AS_TABLE($states)
	`,
		s.placementGroupsPath,
		placementGroupStateStructTypeString()),
		persistence.ValueParam(
			"$states",
			persistence.ListValue(state.structValue()),
		),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) deletePlacementGroup(
	ctx context.Context,
	session *persistence.Session,
	placementGroupID string,
	taskID string,
	deletingAt time.Time,
) (*PlacementGroupMeta, error) {

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
		from placement_groups
		where id = $id
	`, s.placementGroupsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(placementGroupID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	states, err := scanPlacementGroupStates(ctx, res)
	if err != nil {
		return nil, err
	}

	var state placementGroupState

	if len(states) != 0 {
		state = states[0]

		if state.status >= placementGroupStatusDeleting {
			// PlacementGroup already marked as deleting/deleted.

			err = tx.Commit(ctx)
			if err != nil {
				return nil, err
			}

			return state.toPlacementGroupMeta(), nil
		}
	}

	state.id = placementGroupID
	state.status = placementGroupStatusDeleting
	state.deleteTaskID = taskID
	state.deletingAt = deletingAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into placement_groups
		select *
		from AS_TABLE($states)
	`,
		s.placementGroupsPath,
		placementGroupStateStructTypeString()),
		persistence.ValueParam(
			"$states",
			persistence.ListValue(state.structValue()),
		),
	)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return state.toPlacementGroupMeta(), nil
}

func (s *storageYDB) placementGroupDeleted(
	ctx context.Context,
	session *persistence.Session,
	placementGroupID string,
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
		from placement_groups
		where id = $id
	`, s.placementGroupsPath),
		persistence.ValueParam("$id", persistence.UTF8Value(placementGroupID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	states, err := scanPlacementGroupStates(ctx, res)
	if err != nil {
		return err
	}

	if len(states) == 0 {
		// It's possible that placementGroup is already collected.
		return tx.Commit(ctx)
	}

	state := states[0]

	if state.status == placementGroupStatusDeleted {
		// Nothing to do.
		return tx.Commit(ctx)
	}

	if state.status != placementGroupStatusDeleting {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"placementGroup with id %v and status %v can't be deleted",
			placementGroupID,
			placementGroupStatusToString(state.status),
		)
	}

	state.status = placementGroupStatusDeleted
	state.deletedAt = deletedAt

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $states as List<%v>;

		upsert into placement_groups
		select *
		from AS_TABLE($states)
	`,
		s.placementGroupsPath,
		placementGroupStateStructTypeString()),
		persistence.ValueParam(
			"$states",
			persistence.ListValue(state.structValue()),
		),
	)
	if err != nil {
		return err
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $deleted_at as Timestamp;
		declare $placement_group_id as Utf8;

		upsert into deleted (deleted_at, placement_group_id)
		values ($deleted_at, $placement_group_id)
	`, s.placementGroupsPath),
		persistence.ValueParam("$deleted_at", persistence.TimestampValue(deletedAt)),
		persistence.ValueParam(
			"$placement_group_id",
			persistence.UTF8Value(placementGroupID),
		),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) clearDeletedPlacementGroups(
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
	`, s.placementGroupsPath),
		persistence.ValueParam(
			"$deleted_before",
			persistence.TimestampValue(deletedBefore),
		),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var (
				deletedAt        time.Time
				placementGroupID string
			)
			err = res.ScanNamed(
				persistence.OptionalWithDefault("deleted_at", &deletedAt),
				persistence.OptionalWithDefault("placement_group_id", &placementGroupID),
			)
			if err != nil {
				return err
			}

			_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				declare $deleted_at as Timestamp;
				declare $placement_group_id as Utf8;
				declare $status as Int64;

				delete from placement_groups
				where id = $placement_group_id and status = $status;

				delete from deleted
				where deleted_at = $deleted_at and placement_group_id = $placement_group_id
			`, s.placementGroupsPath),
				persistence.ValueParam(
					"$deleted_at",
					persistence.TimestampValue(deletedAt),
				),
				persistence.ValueParam(
					"$placement_group_id",
					persistence.UTF8Value(placementGroupID),
				),
				persistence.ValueParam(
					"$status",
					persistence.Int64Value(int64(placementGroupStatusDeleted)),
				),
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *storageYDB) listPlacementGroups(
	ctx context.Context,
	session *persistence.Session,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	return listResources(
		ctx,
		session,
		s.placementGroupsPath,
		"placement_groups",
		folderID,
		creatingBefore,
	)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) CreatePlacementGroup(
	ctx context.Context,
	placementGroup PlacementGroupMeta,
) (*PlacementGroupMeta, error) {

	var created *PlacementGroupMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			created, err = s.createPlacementGroup(ctx, session, placementGroup)
			return err
		},
	)
	return created, err
}

func (s *storageYDB) PlacementGroupCreated(
	ctx context.Context,
	placementGroup PlacementGroupMeta,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.placementGroupCreated(
				ctx,
				session,
				placementGroup,
			)
		},
	)
}

func (s *storageYDB) GetPlacementGroupMeta(
	ctx context.Context,
	placementGroupID string,
) (*PlacementGroupMeta, error) {

	var placementGroup *PlacementGroupMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			placementGroup, err =
				s.getPlacementGroupMeta(ctx, session, placementGroupID)
			return err
		},
	)
	return placementGroup, err
}

func (s *storageYDB) CheckPlacementGroupReady(
	ctx context.Context,
	placementGroupID string,
) (*PlacementGroupMeta, error) {

	var placementGroup *PlacementGroupMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			placementGroup, err = s.checkPlacementGroupReady(
				ctx,
				session,
				placementGroupID,
			)
			return err
		},
	)
	return placementGroup, err
}

func (s *storageYDB) DeletePlacementGroup(
	ctx context.Context,
	placementGroupID string,
	taskID string,
	deletingAt time.Time,
) (*PlacementGroupMeta, error) {

	var placementGroup *PlacementGroupMeta

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			placementGroup, err = s.deletePlacementGroup(
				ctx,
				session,
				placementGroupID,
				taskID,
				deletingAt,
			)
			return err
		},
	)
	return placementGroup, err
}

func (s *storageYDB) PlacementGroupDeleted(
	ctx context.Context,
	placementGroupID string,
	deletedAt time.Time,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.placementGroupDeleted(
				ctx,
				session,
				placementGroupID,
				deletedAt,
			)
		},
	)
}

func (s *storageYDB) ClearDeletedPlacementGroups(
	ctx context.Context,
	deletedBefore time.Time,
	limit int,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.clearDeletedPlacementGroups(
				ctx,
				session,
				deletedBefore,
				limit,
			)
		},
	)
}

func (s *storageYDB) ListPlacementGroups(
	ctx context.Context,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	var ids []string

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			ids, err = s.listPlacementGroups(ctx, session, folderID, creatingBefore)
			return err
		},
	)
	return ids, err
}

////////////////////////////////////////////////////////////////////////////////

func createPlacementGroupsYDBTables(
	ctx context.Context,
	folder string,
	db *persistence.YDBClient,
	dropUnusedColumns bool,
) error {

	logging.Info(
		ctx,
		"Creating tables for placementGroups in %v",
		db.AbsolutePath(folder),
	)

	err := db.CreateOrAlterTable(
		ctx,
		folder,
		"placement_groups",
		placementGroupStateTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created placement_groups table")

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		"deleted",
		persistence.NewCreateTableDescription(
			persistence.WithColumn(
				"deleted_at",
				persistence.Optional(persistence.TypeTimestamp)),
			persistence.WithColumn(
				"placement_group_id",
				persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn(
				"deleted_at",
				"placement_group_id",
			),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created deleted table")

	logging.Info(ctx, "Created tables for placementGroups")

	return nil
}

func dropPlacementGroupsYDBTables(
	ctx context.Context,
	folder string,
	db *persistence.YDBClient,
) error {

	logging.Info(
		ctx,
		"Dropping tables for placementGroups in %v",
		db.AbsolutePath(folder),
	)

	err := db.DropTable(ctx, folder, "placement_groups")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped placement_groups table")

	err = db.DropTable(ctx, folder, "deleted")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped deleted table")

	logging.Info(ctx, "Dropped tables for placement_groups")

	return nil
}
