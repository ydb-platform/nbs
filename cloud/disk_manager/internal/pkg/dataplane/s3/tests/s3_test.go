package tests

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/s3"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url"
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

	urli := "kek"

	s3_client, err := test.NewS3Client()
	require.NoError(t, err)

	err = s3_client.CreateBucket(ctx, "bucket")
	require.NoError(t, err)

	uploadId, err := s3_client.CreateMultipartUpload(ctx, "bucket", "key")
	require.NoError(t, err)

	logging.Info(ctx, "upload id is %v", uploadId)

	target, err := s3.NewS3Target(
		ctx,
		s3_client,
		"bucket",
		"key",
		uploadId,
	)
	require.NoError(t, err)
	defer target.Close(ctx)

	chunks := make([]dataplane_common.Chunk, 0)
	for i := uint32(0); i < chunkCount; i++ {
		var chunk dataplane_common.Chunk

		if rand.Intn(2) == 1 {
			data := make([]byte, chunkSize)
			rand.Read(data)
			chunk = dataplane_common.Chunk{Index: i, Data: data}
		} else {
			// Zero chunk.
			chunk = dataplane_common.Chunk{Index: i, Zero: true}
		}

		err = target.Write(ctx, chunk)
		require.NoError(t, err)

		chunks = append(chunks, chunk)
	}

	source, err := url.NewURLSource(
		ctx,
		defaultHTTPClientTimeout,
		defaultHTTPClientMinRetryTimeout,
		defaultHTTPClientMaxRetryTimeout,
		defaultHTTPClientMaxRetries,
		urli,
		chunkSize,
	)
	require.NoError(t, err)
	defer source.Close(ctx)

	checkChunks(t, ctx, source, chunks)
}
