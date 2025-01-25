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

	storage := newAvailabilityMonitoringStorage(t, ctx, db)

	createdAt := time.Date(2024, 02, 24, 13, 0, 0, 0, time.Local)
	err = storage.UpdateSuccessRate(ctx, "component", "host", 0.2, createdAt)
	require.NoError(t, err)

	results, err := storage.GetAvailabilityMonitoringResults(ctx, "component", "host")
	require.NoError(t, err)

	require.EqualValues(t, []availabilityMonitoringResult{
		{
			component:   "component",
			host:        "host",
			createdAt:   createdAt,
			successRate: 0.2,
		},
	}, results)
}
