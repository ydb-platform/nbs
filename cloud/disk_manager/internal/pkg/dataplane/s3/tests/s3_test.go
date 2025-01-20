package tests

import (
	"context"
	"encoding/binary"
	"math/rand"
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
	chunkCount                       = uint64(30)
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestReadWrite(t *testing.T) {
	ctx := newContext()

	s3Client, err := test.NewS3Client()
	require.NoError(t, err)

	err = s3Client.CreateBucket(ctx, "bucket")
	require.NoError(t, err)

	uploadId, err := s3Client.CreateMultipartUpload(ctx, "bucket", "key")
	require.NoError(t, err)

	completedParts := []*aws_s3.CompletedPart{}
	target := s3.NewS3Target(
		ctx,
		s3Client,
		"bucket",
		"key",
		uploadId,
		&completedParts,
	)
	defer target.Close(ctx)

	expectedData := make([]byte, 0)
	for i := uint64(0); i < chunkCount; i++ {
		var chunk dataplane_common.Chunk

		data := make([]byte, s3.ChunkSize)
		if rand.Intn(2) == 1 {
			rand.Read(data)
			chunk = dataplane_common.Chunk{Index: uint32(i), Data: data}
			expectedData = append(expectedData, data...)
		} else {
			// Zero chunk.
			chunk = dataplane_common.Chunk{Index: uint32(i), Zero: true}
			expectedData = append(expectedData, data...)
		}

		err = target.Write(ctx, chunk)
		require.NoError(t, err)
	}

	err = s3Client.CompleteMultipartUpload(ctx, "bucket", "key", uploadId, completedParts)
	require.NoError(t, err)

	url, err := s3Client.Presign(ctx, "bucket", "key")
	require.NoError(t, err)

	source, err := common.NewURLReader(
		ctx,
		defaultHTTPClientTimeout,
		defaultHTTPClientMinRetryTimeout,
		defaultHTTPClientMaxRetryTimeout,
		defaultHTTPClientMaxRetries,
		url,
	)
	require.NoError(t, err)

	actualData := make([]byte, s3.ChunkSize*chunkCount)
	err = source.ReadBinary(ctx, 0, s3.ChunkSize*chunkCount, binary.BigEndian, &actualData)
	require.NoError(t, err)

	require.EqualValues(t, expectedData, actualData)
}
