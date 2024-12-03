package tests

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/s3"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

const (
	defaultHTTPClientTimeout         = time.Minute
	defaultHTTPClientMinRetryTimeout = time.Second
	defaultHTTPClientMaxRetryTimeout = 8 * time.Second
	defaultHTTPClientMaxRetries      = 5
	chunkSize                        = uint64(1024 * 4096) // 4 MiB
	chunkCount                       = uint32(33)
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func checkChunks(
	t *testing.T,
	ctx context.Context,
	source dataplane_common.Source,
	expectedChunks []dataplane_common.Chunk,
) {

	for _, expected := range expectedChunks {
		actual := dataplane_common.Chunk{
			Index: expected.Index,
			Data:  make([]byte, chunkSize),
		}
		err := source.Read(ctx, &actual)
		require.NoError(t, err)

		require.Equal(t, expected.Index, actual.Index)
		require.Equal(t, expected.Zero, actual.Zero)
		if !expected.Zero {
			require.Equal(t, expected.Data, actual.Data)
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestReadWrite(t *testing.T) {
	ctx := newContext()

	urli := fmt.Sprintf("http://localhost:%s/bucket/key", os.Getenv("DISK_MANAGER_RECIPE_S3_PORT"))

	s3_client, err := test.NewS3Client()
	require.NoError(t, err)

	err = s3_client.CreateBucket(ctx, "bucket")
	require.NoError(t, err)

	uploadId, err := s3_client.CreateMultipartUpload(ctx, "bucket", "key")
	require.NoError(t, err)

	logging.Info(ctx, "upload id is %v", uploadId)

	completedParts := []*aws_s3.CompletedPart{}
	target, err := s3.NewS3Target(
		ctx,
		s3_client,
		"bucket",
		"key",
		uploadId,
		&completedParts,
	)
	require.NoError(t, err)
	defer target.Close(ctx)

	list, err := s3_client.List(ctx, "bucket", "key")
	for _, upload := range list {
		logging.Info(ctx, "Upload ID: %s, Key: %s\n", *upload.UploadId, *upload.Key)
	}

	chunks := make([]dataplane_common.Chunk, 0)
	for i := uint32(0); i < 1; i++ {
		var chunk dataplane_common.Chunk

		// if rand.Intn(2) == 1 {
		data := make([]byte, chunkSize)
		rand.Read(data)
		chunk = dataplane_common.Chunk{Index: i, Data: data}
		// } else {
		// 	// Zero chunk.
		// 	chunk = dataplane_common.Chunk{Index: i, Zero: true}
		// }

		err = target.Write(ctx, chunk)
		require.NoError(t, err)

		chunks = append(chunks, chunk)
	}
	// target.Close(ctx)
	// close(completedParts)

	// var completedPartsarr []*aws_s3.CompletedPart
	// for part := range completedParts {
	// 	completedPartsarr = append(completedPartsarr, part)
	// }

	err = s3_client.CompleteMultipartUpload(ctx, "bucket", "key", uploadId, completedParts)
	require.NoError(t, err)

	urli, err = s3_client.Presign(ctx, "bucket", "key")
	require.NoError(t, err)

	source, err := common.NewURLReader(
		ctx,
		defaultHTTPClientTimeout,
		defaultHTTPClientMinRetryTimeout,
		defaultHTTPClientMaxRetryTimeout,
		defaultHTTPClientMaxRetries,
		urli,
	)
	require.NoError(t, err)

	data := make([]byte, 4194304)
	err = source.ReadBinary(ctx, 0, 4194304, binary.BigEndian, &data)
	require.NoError(t, err)

	require.EqualValues(t, chunks[0].Data, data)
}
