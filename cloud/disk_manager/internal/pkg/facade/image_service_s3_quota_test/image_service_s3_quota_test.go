package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
)

////////////////////////////////////////////////////////////////////////////////

func TestImageServiceCreateImageFromDiskWithS3StorageClassQuotaFallback(t *testing.T) {
	ctx := testcommon.NewContext()

	testcommon.CreateImage(
		t,
		ctx,
		t.Name(),
		4*1024*1024, // imageSize
		"folder",
		false, // pooled
	)

	quotaExceededCounters := testcommon.GetCountersDataplane(
		t,
		"errors_quotaExceeded",
		map[string]string{
			"call":      "PutObject",
			"component": "s3_client",
		},
	)

	var quotaExceededCount float64
	for _, counter := range quotaExceededCounters {
		quotaExceededCount += counter
	}

	require.NotZero(t, quotaExceededCount)
}
