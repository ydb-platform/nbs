package resources

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func requirePlacementGroupsAreEqual(
	t *testing.T,
	expected PlacementGroupMeta,
	actual PlacementGroupMeta,
) {

	// TODO: Get rid of boilerplate.
	require.Equal(t, expected.ID, actual.ID)
	require.Equal(t, expected.ZoneID, actual.ZoneID)
	require.Equal(t, expected.PlacementStrategy, actual.PlacementStrategy)
	require.Equal(t, expected.PlacementPartitionCount, actual.PlacementPartitionCount)
	require.True(t, proto.Equal(expected.CreateRequest, actual.CreateRequest))
	require.Equal(t, expected.CreateTaskID, actual.CreateTaskID)
	if !expected.CreatingAt.IsZero() {
		require.WithinDuration(t, expected.CreatingAt, actual.CreatingAt, time.Microsecond)
	}
	if !expected.CreatedAt.IsZero() {
		require.WithinDuration(t, expected.CreatedAt, actual.CreatedAt, time.Microsecond)
	}
	require.Equal(t, expected.CreatedBy, actual.CreatedBy)
	require.Equal(t, expected.DeleteTaskID, actual.DeleteTaskID)
}

////////////////////////////////////////////////////////////////////////////////

func TestPlacementGroupsCreatePlacementGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	placementGroup := PlacementGroupMeta{
		ID:                      "placementGroup",
		ZoneID:                  "zone",
		PlacementStrategy:       types.PlacementStrategy_PLACEMENT_STRATEGY_PARTITION,
		PlacementPartitionCount: 2,

		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreatePlacementGroup(ctx, placementGroup)
	require.NoError(t, err)
	require.NotNil(t, actual)

	expected := placementGroup
	expected.CreateRequest = nil
	requirePlacementGroupsAreEqual(t, expected, *actual)

	// Check idempotency.
	actual, err = storage.CreatePlacementGroup(ctx, placementGroup)
	require.NoError(t, err)
	require.NotNil(t, actual)

	placementGroup.CreatedAt = time.Now()
	err = storage.PlacementGroupCreated(ctx, placementGroup)
	require.NoError(t, err)

	// Check idempotency.
	err = storage.PlacementGroupCreated(ctx, placementGroup)
	require.NoError(t, err)

	// Check idempotency.
	actual, err = storage.CreatePlacementGroup(ctx, placementGroup)
	require.NoError(t, err)
	require.NotNil(t, actual)

	expected = placementGroup
	expected.CreateRequest = nil
	requirePlacementGroupsAreEqual(t, expected, *actual)

	placementGroup.CreateTaskID = "other"
	actual, err = storage.CreatePlacementGroup(ctx, placementGroup)
	require.NoError(t, err)
	require.Nil(t, actual)
}

func TestPlacementGroupsDeletePlacementGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	placementGroup := PlacementGroupMeta{
		ID: "placementGroup",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreatePlacementGroup(ctx, placementGroup)
	require.NoError(t, err)
	require.NotNil(t, actual)

	expected := placementGroup
	expected.CreateRequest = nil
	expected.DeleteTaskID = "delete"

	actual, err = storage.DeletePlacementGroup(
		ctx,
		placementGroup.ID,
		"delete",
		time.Now(),
	)
	require.NoError(t, err)
	requirePlacementGroupsAreEqual(t, expected, *actual)

	placementGroup.CreatedAt = time.Now()
	err = storage.PlacementGroupCreated(ctx, placementGroup)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	// Check idempotency.
	actual, err = storage.DeletePlacementGroup(
		ctx,
		placementGroup.ID,
		"delete",
		time.Now(),
	)
	require.NoError(t, err)
	requirePlacementGroupsAreEqual(t, expected, *actual)

	_, err = storage.CreatePlacementGroup(ctx, placementGroup)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.PlacementGroupDeleted(ctx, placementGroup.ID, time.Now())
	require.NoError(t, err)

	// Check idempotency.
	actual, err = storage.DeletePlacementGroup(
		ctx,
		placementGroup.ID,
		"delete",
		time.Now(),
	)
	require.NoError(t, err)
	requirePlacementGroupsAreEqual(t, expected, *actual)

	_, err = storage.CreatePlacementGroup(ctx, placementGroup)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.PlacementGroupCreated(ctx, placementGroup)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
}

func TestPlacementGroupsDeleteNonexistentPlacementGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	placementGroup := PlacementGroupMeta{
		ID:           "placementGroup",
		DeleteTaskID: "delete",
	}

	err = storage.PlacementGroupDeleted(ctx, placementGroup.ID, time.Now())
	require.NoError(t, err)

	created := placementGroup
	created.CreatedAt = time.Now()
	err = storage.PlacementGroupCreated(ctx, created)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	deletingAt := time.Now()
	actual, err := storage.DeletePlacementGroup(
		ctx,
		placementGroup.ID,
		"delete",
		deletingAt,
	)
	require.NoError(t, err)
	requirePlacementGroupsAreEqual(t, placementGroup, *actual)

	// Check idempotency.
	deletingAt = deletingAt.Add(time.Second)
	actual, err = storage.DeletePlacementGroup(
		ctx,
		placementGroup.ID,
		"delete",
		deletingAt,
	)
	require.NoError(t, err)
	requirePlacementGroupsAreEqual(t, placementGroup, *actual)

	_, err = storage.CreatePlacementGroup(ctx, placementGroup)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.PlacementGroupDeleted(ctx, placementGroup.ID, time.Now())
	require.NoError(t, err)
}

func TestPlacementGroupsClearDeletedPlacementGroups(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	deletedAt := time.Now()
	deletedBefore := deletedAt.Add(-time.Microsecond)

	err = storage.ClearDeletedPlacementGroups(ctx, deletedBefore, 10)
	require.NoError(t, err)

	placementGroup := PlacementGroupMeta{
		ID: "placementGroup",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreatePlacementGroup(ctx, placementGroup)
	require.NoError(t, err)
	require.NotNil(t, actual)

	_, err = storage.DeletePlacementGroup(
		ctx,
		placementGroup.ID,
		"delete",
		deletedAt,
	)
	require.NoError(t, err)

	err = storage.PlacementGroupDeleted(ctx, placementGroup.ID, deletedAt)
	require.NoError(t, err)

	err = storage.ClearDeletedPlacementGroups(ctx, deletedBefore, 10)
	require.NoError(t, err)

	_, err = storage.CreatePlacementGroup(ctx, placementGroup)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	deletedBefore = deletedAt.Add(time.Microsecond)
	err = storage.ClearDeletedPlacementGroups(ctx, deletedBefore, 10)
	require.NoError(t, err)

	actual, err = storage.CreatePlacementGroup(ctx, placementGroup)
	require.NoError(t, err)
	require.NotNil(t, actual)
}

func TestPlacementGroupsGetPlacementGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	placementGroupID := t.Name()

	d, err := storage.GetPlacementGroupMeta(ctx, placementGroupID)
	require.NoError(t, err)
	require.Nil(t, d)

	placementGroup := PlacementGroupMeta{
		ID:                placementGroupID,
		ZoneID:            "zone",
		PlacementStrategy: types.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD,

		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreatePlacementGroup(ctx, placementGroup)
	require.NoError(t, err)
	require.NotNil(t, actual)

	placementGroup.CreateRequest = nil

	d, err = storage.GetPlacementGroupMeta(ctx, placementGroupID)
	require.NoError(t, err)
	require.NotNil(t, d)
	requirePlacementGroupsAreEqual(t, placementGroup, *d)
}
