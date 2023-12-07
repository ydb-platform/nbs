package test

import (
	"context"
	"fmt"
	"os"

	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func NewS3Client() (*persistence.S3Client, error) {
	endpoint := fmt.Sprintf("http://localhost:%s", os.Getenv("DISK_MANAGER_RECIPE_S3_PORT"))
	credentials := persistence.NewS3Credentials("test", "test")
	return persistence.NewS3Client(endpoint, "test", credentials)
}

func NewContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

func NewS3Key(config *snapshot_config.SnapshotConfig, chunkID string) string {
	return fmt.Sprintf(
		"%v/%v",
		config.GetChunkBlobsS3KeyPrefix(),
		chunkID,
	)
}
