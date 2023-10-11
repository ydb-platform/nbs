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
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/library/go/test/yatest"
)

////////////////////////////////////////////////////////////////////////////////

func getVmdkImageFileURLUbuntu2204() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_VMDK_UBUNTU2204_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func getVmdkExpectedMapFileUbuntu2204() string {
	// Result of 'qemu-img map --output=json ubuntu-22.04-jammy-server-cloudimg-amd64.vmdk'.
	return yatest.SourcePath("cloud/disk_manager/internal/pkg/dataplane/url/vmdk/tests/data/qemuimg_map_ubuntu2204.json")
}

////////////////////////////////////////////////////////////////////////////////

func getVmdkReader(
	t *testing.T,
	ctx context.Context,
	reader common.Reader,
) common.ImageMapReader {

	vmdkReader := vmdk.NewImageMapReader(reader)

	for {
		err := vmdkReader.ReadHeader(ctx)
		if !errors.CanRetry(err) {
			require.NoError(t, err)
			break
		}
	}

	return vmdkReader
}

////////////////////////////////////////////////////////////////////////////////

func TestVMDKMapImageUbuntu2204(t *testing.T) {
	common_testing.MapImageTest(
		t,
		getVmdkImageFileURLUbuntu2204(),
		getVmdkExpectedMapFileUbuntu2204(),
		getVmdkReader,
	)
}
