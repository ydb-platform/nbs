package test

import (
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	s3_fault_proxy "github.com/ydb-platform/nbs/cloud/disk_manager/test/mocks/s3_fault_proxy"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

// NewFaultyS3Client routes only this client through a loopback fault proxy.
// SDK retries are disabled so each injected failure reaches the task runner.
// This does not change the recipe S3 or other clients using it.
func NewFaultyS3Client(t *testing.T) (*persistence.S3Client, *s3_fault_proxy.Proxy) {
	t.Helper()
	port := os.Getenv("DISK_MANAGER_RECIPE_S3_PORT")
	require.NotEmpty(t, port, "the Disk Manager S3 recipe must be running")
	proxy, err := s3_fault_proxy.New(fmt.Sprintf("http://localhost:%s", port))
	require.NoError(t, err)
	// Registered before server.Close: LIFO cleanup first drains the front
	// server, then closes idle upstream connections. Test-owned hold gates must
	// be released by a defer or a later Cleanup before either of these runs.
	t.Cleanup(proxy.Close)
	server := httptest.NewServer(proxy)
	t.Cleanup(server.Close)
	client, err := persistence.NewS3Client(
		server.URL,
		"test",
		persistence.NewS3Credentials("test", "test"),
		5*time.Second,
		metrics.NewEmptyRegistry(),
		0,   // maxRetriableErrorCount
		nil, // availabilityMonitoring
		nil, // tokenProvider
	)
	require.NoError(t, err)
	return client, proxy
}
