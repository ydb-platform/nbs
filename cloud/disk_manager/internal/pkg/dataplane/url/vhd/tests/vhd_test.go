package tests

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
	common_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common/testing"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/vhd"
)

////////////////////////////////////////////////////////////////////////////////

func getVHDImageFileURL() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_VHD_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func getVHDImageMapFile() string {
	return os.Getenv("DISK_MANAGER_RECIPE_VHD_IMAGE_MAP_FILE")
}

////////////////////////////////////////////////////////////////////////////////

func getVHDReader(
	t *testing.T,
	ctx context.Context,
	reader common.Reader,
) common.ImageMapReader {

	return vhd.NewImageMapReader(reader)
}

////////////////////////////////////////////////////////////////////////////////

func TestVHDMapImage(t *testing.T) {
	common_testing.MapImageTest(
		t,
		getVHDImageFileURL(),
		getVHDImageMapFile(),
		getVHDReader,
	)
}