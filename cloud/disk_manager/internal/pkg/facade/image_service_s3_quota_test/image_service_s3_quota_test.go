package tests

import (
	"testing"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
)

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
}
