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
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/library/go/test/yatest"
)

////////////////////////////////////////////////////////////////////////////////

func getQCOW2ImageFileURLUbuntu1604() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1604_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func getQCOW2ExpectedMapFileUbuntu1604() string {
	// Result of 'qemu-img map --output=json ubuntu1604-ci-stable'.
	return yatest.SourcePath("cloud/disk_manager/test/images/recipe/data/qemuimg_map_ubuntu1604.json")
}

func getQCOW2ImageFileURLUbuntu1804() string {
	port := os.Getenv("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1804_IMAGE_FILE_SERVER_PORT")
	return fmt.Sprintf("http://localhost:%v", port)
}

func getQCOW2ExpectedMapFileUbuntu1804() string {
	// Result of 'qemu-img map --output=json ubuntu-18.04-minimal-cloudimg-amd64.img'.
	return yatest.SourcePath("cloud/disk_manager/test/images/recipe/data/qemuimg_map_ubuntu1804.json")
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
		getQCOW2ImageFileURLUbuntu1604(),
		getQCOW2ExpectedMapFileUbuntu1604(),
		getQCOW2Reader,
	)
}

func TestQCOW2MapImageUbuntu1804(t *testing.T) {
	common_testing.MapImageTest(
		t,
		getQCOW2ImageFileURLUbuntu1804(),
		getQCOW2ExpectedMapFileUbuntu1804(),
		getQCOW2Reader,
	)
}
