package persistence

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
)

////////////////////////////////////////////////////////////////////////////////

func TestAvailabilityMonitoringStorageYDBUpdateSuccessRate(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx, empty.NewRegistry())
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newAvailabilityMonitoringStorage(
		t,
		ctx,
		db,
		10*time.Second, // banInterval
		1,              // maxBanHostsCount
	)

	createdAt := time.Date(2024, 02, 24, 13, 0, 0, 0, time.Local)
	err = storage.UpdateSuccessRate(ctx, "component", "host", 0.2, createdAt)
	require.NoError(t, err)

	results, err := storage.GetAvailabilityMonitoringResults(
		ctx,
		"host",
		"component",
		createdAt.Add(-10*time.Second),
	)
	require.NoError(t, err)

	require.EqualValues(t, []AvailabilityMonitoringResult{
		{
			Component:   "component",
			Host:        "host",
			CreatedAt:   createdAt,
			SuccessRate: 0.2,
		},
	}, results)
}

func TestAvailabilityMonitoringStorageYDBUpdateComponentAvailability(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx, empty.NewRegistry())
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newAvailabilityMonitoringStorage(
		t,
		ctx,
		db,
		10*time.Second, // banInterval
		1,              // maxBanHostsCount
	)

	err = storage.UpdateComponentAvailability(ctx, "host", "component1", false)
	require.NoError(t, err)

	err = storage.UpdateComponentAvailability(ctx, "host", "component2", true)
	require.NoError(t, err)

	results, err := storage.GetComponentsAvailabilityResults(
		ctx,
		"host",
	)
	require.NoError(t, err)

	require.Equal(t, 2, len(results))
	require.EqualValues(t, "host", results[0].Host)
	require.EqualValues(t, "component1", results[0].Component)
	require.EqualValues(t, false, results[0].Availability)

	require.EqualValues(t, "host", results[1].Host)
	require.EqualValues(t, "component2", results[1].Component)
	require.EqualValues(t, true, results[1].Availability)
}

func TestAvailabilityMonitoringStorageYDBUpdateComponentsBackFromUnavailable(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx, empty.NewRegistry())
	require.NoError(t, err)
	defer db.Close(ctx)

	banInterval := 10 * time.Second
	storage := newAvailabilityMonitoringStorage(
		t,
		ctx,
		db,
		banInterval, // banInterval
		1,           // maxBanHostsCount
	)

	err = storage.UpdateComponentAvailability(ctx, "host", "component1", false)
	require.NoError(t, err)

	err = storage.UpdateComponentAvailability(ctx, "host", "component2", false)
	require.NoError(t, err)

	err = storage.UpdateComponentAvailability(ctx, "host", "component3", true)
	require.NoError(t, err)

	time.Sleep(banInterval)

	err = storage.UpdateComponentsBackFromUnavailable(ctx, "host", "component1")
	require.NoError(t, err)

	err = storage.UpdateComponentsBackFromUnavailable(ctx, "host", "component2")
	require.NoError(t, err)

	results, err := storage.GetComponentsAvailabilityResults(
		ctx,
		"host",
	)
	require.NoError(t, err)

	require.Equal(t, 3, len(results))

	require.EqualValues(t, true, results[0].Availability)
	require.EqualValues(t, true, results[1].Availability)
	require.EqualValues(t, true, results[2].Availability)
}
