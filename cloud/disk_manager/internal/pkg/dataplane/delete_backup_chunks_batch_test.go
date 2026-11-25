package dataplane

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

func TestDeleteBackupChunksTaskConfirmsPartialBatches(t *testing.T) {
	ctx := test.NewContext()
	registry := metrics.NewEmptyRegistry()
	var retry atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.True(t, r.URL.Query().Has("delete"))
		assert.Equal(t, "/bucket", r.URL.Path)

		var request struct {
			Keys []string `xml:"Object>Key"`
		}
		if !assert.NoError(t, xml.NewDecoder(r.Body).Decode(&request)) ||
			!assert.NotEmpty(t, request.Keys) {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}

		if !retry.Load() && request.Keys[0] == "prefix/chunks/0" {
			// The whole first request fails. Subsequent requests must still run.
			http.Error(w, "<Error><Code>AccessDenied</Code></Error>", http.StatusForbidden)
			return
		}
		fmt.Fprint(w, "<DeleteResult>")
		for _, key := range request.Keys {
			if !retry.Load() && key == "prefix/chunks/100" {
				fmt.Fprintf(w, "<Error><Key>%s</Key><Code>InternalError</Code></Error>", key)
			} else {
				fmt.Fprintf(w, "<Deleted><Key>%s</Key></Deleted>", key)
			}
		}
		fmt.Fprint(w, "</DeleteResult>")
	}))
	defer server.Close()

	client, err := persistence.NewS3Client(
		server.URL, "region", persistence.NewS3Credentials("test", "test"),
		time.Second, registry, 0, nil, nil, 0,
	)
	require.NoError(t, err)
	follower, err := backup.NewS3(client, "bucket", "prefix", "", nil, 0, registry)
	require.NoError(t, err)

	ids := make([]string, 201)
	for i := range ids {
		ids[i] = strconv.Itoa(i)
	}

	storage := storage_mocks.NewStorageMock()
	storage.On("GetBackupChunksToDelete", ctx, len(ids)).Return(ids, nil).Once()
	storage.On("BackupChunksDeleted", ctx, ids[101:]).Return(nil).Once()
	task := deleteBackupChunksTask{
		storage:       storage,
		backupS3:      follower,
		batchSize:     len(ids),
		inflightLimit: 1,
		registry:      registry,
	}
	err = task.Run(ctx, nil)
	require.Error(t, err)
	require.False(t, errors.Is(err, errors.NewInterruptExecutionError()))
	retry.Store(true)
	pending := ids[:101]
	storage.On("GetBackupChunksToDelete", ctx, len(ids)).Return(pending, nil).Once()
	storage.On("BackupChunksDeleted", ctx, pending).Return(nil).Once()
	storage.On("GetBackupChunksToDelete", ctx, len(ids)).Return([]string{}, nil).Once()
	err = task.Run(ctx, nil)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	storage.AssertExpectations(t)
}
