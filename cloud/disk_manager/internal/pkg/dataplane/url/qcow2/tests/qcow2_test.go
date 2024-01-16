package tests

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
	common_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common/testing"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/qcow2"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func getQCOW2Ubuntu1604ImageFileURL() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1604_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func getQCOW2Ubuntu1604ImageMapFile() string {
	return os.Getenv("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1604_IMAGE_MAP_FILE")
}

func getQCOW2Ubuntu1804ImageFileURL() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1804_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func getQCOW2Ubuntu1804ImageMapFile() string {
	return os.Getenv("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1804_IMAGE_MAP_FILE")
}

////////////////////////////////////////////////////////////////////////////////

func getQCOW2Reader(
	t *testing.T,
	ctx context.Context,
	reader common.Reader,
) common.ImageMapReader {

	qcow2Reader := qcow2.NewImageMapReader(reader)

	for {
		err := qcow2Reader.ReadHeader(ctx)
		if !errors.CanRetry(err) {
			require.NoError(t, err)
			break
		}
	}

	return qcow2Reader
}

////////////////////////////////////////////////////////////////////////////////

func TestQCOW2MapImageUbuntu1604(t *testing.T) {
	common_testing.MapImageTest(
		t,
		getQCOW2Ubuntu1604ImageFileURL(),
		getQCOW2Ubuntu1604ImageMapFile(),
		getQCOW2Reader,
	)
}

func TestQCOW2MapImageUbuntu1804(t *testing.T) {
	common_testing.MapImageTest(
		t,
		getQCOW2Ubuntu1804ImageFileURL(),
		getQCOW2Ubuntu1804ImageMapFile(),
		getQCOW2Reader,
	)
}
