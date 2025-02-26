package test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func NewS3Client() (*persistence.S3Client, error) {
	endpoint := fmt.Sprintf("http://localhost:%s", os.Getenv("DISK_MANAGER_RECIPE_S3_PORT"))
	credentials := persistence.NewS3Credentials("test", "test")
	callTimeout := 600 * time.Second
	return persistence.NewS3Client(
		endpoint,
		"test",
		credentials,
		callTimeout,
		metrics.NewEmptyRegistry(),
		100, // maxRetriableErrorCount
	)
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

////////////////////////////////////////////////////////////////////////////////

func FillTarget(
	t *testing.T,
	ctx context.Context,
	target dataplane_common.Target,
	chunkCount uint32,
	chunkSize uint32,
) []dataplane_common.Chunk {

	rand.Seed(time.Now().UnixNano())

	chunks := make([]dataplane_common.Chunk, 0)
	for i := uint32(0); i < chunkCount; i++ {
		dice := rand.Intn(3)

		switch dice {
		case 0:
			data := make([]byte, chunkSize)
			rand.Read(data)
			chunk := dataplane_common.Chunk{Index: i, Data: data}
			chunks = append(chunks, chunk)
		case 1:
			// Zero chunk.
			chunk := dataplane_common.Chunk{Index: i, Zero: true}
			chunks = append(chunks, chunk)
		}
	}

	logging.Info(ctx, "Filling resource...")

	for _, chunk := range chunks {
		err := target.Write(ctx, chunk)
		require.NoError(t, err)
	}

	return chunks
}
