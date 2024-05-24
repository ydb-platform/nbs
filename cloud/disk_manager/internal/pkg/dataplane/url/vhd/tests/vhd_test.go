package tests

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
	common_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common/testing"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/vhd"
)

////////////////////////////////////////////////////////////////////////////////

func getVHDImageFileURL() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_VHD_DYNAMIC_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func getVHDImageMapFile() string {
	return os.Getenv("DISK_MANAGER_RECIPE_VHD_DYNAMIC_IMAGE_MAP_FILE")
}

func getVHDUbuntu1604ImageFileURL() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_VHD_UBUNTU1604_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func getVHDUbuntu1604ImageMapFile() string {
	return os.Getenv("DISK_MANAGER_RECIPE_VHD_UBUNTU1604_IMAGE_MAP_FILE")
}

////////////////////////////////////////////////////////////////////////////////

func getVHDReader(
	t *testing.T,
	ctx context.Context,
	reader common.Reader,
) common.ImageMapReader {

	imageMapReader, err := vhd.NewImageMapReader(ctx, reader)
	require.NoError(t, err)
	return imageMapReader
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

func TestVHDMapImageUbuntu1604(t *testing.T) {
	common_testing.MapImageTest(
		t,
		getVHDUbuntu1604ImageFileURL(),
		getVHDUbuntu1604ImageMapFile(),
		getVHDReader,
	)
}
