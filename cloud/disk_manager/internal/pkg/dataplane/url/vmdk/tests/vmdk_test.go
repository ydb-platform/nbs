package tests

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
	common_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common/testing"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/vmdk"
)

////////////////////////////////////////////////////////////////////////////////

func getVMDKUbuntu2204ImageFileURL() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_VMDK_UBUNTU2204_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func getVMDKUbuntu2204ImageMapFile() string {
	return os.Getenv("DISK_MANAGER_RECIPE_VMDK_UBUNTU2204_IMAGE_MAP_FILE")
}

func getVMDKWindowsImageFileURL() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_VMDK_WINDOWS_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func getVMDKWindowsImageMapFile() string {
	return os.Getenv("DISK_MANAGER_RECIPE_VMDK_WINDOWS_IMAGE_MAP_FILE")
}

////////////////////////////////////////////////////////////////////////////////

func getVMDKReader(
	t *testing.T,
	ctx context.Context,
	reader common.Reader,
) common.ImageMapReader {

	imageMapReader, err := vmdk.NewImageMapReader(ctx, reader)
	require.NoError(t, err)
	return imageMapReader
}

////////////////////////////////////////////////////////////////////////////////

func TestVMDKMapImageUbuntu2204(t *testing.T) {
	common_testing.MapImageTest(
		t,
		getVMDKUbuntu2204ImageFileURL(),
		getVMDKUbuntu2204ImageMapFile(),
		getVMDKReader,
	)
}

func TestVMDKMapImageWindows(t *testing.T) {
	common_testing.MapImageTest(
		t,
		getVMDKWindowsImageFileURL(),
		getVMDKWindowsImageMapFile(),
		getVMDKReader,
	)
}
