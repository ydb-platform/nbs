package exporter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/stretchr/testify/require"

	common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

type flakyS3Source struct {
	*s3TestSource
}

func (s flakyS3Source) CheckSnapshotReady(ctx context.Context, id string) (storage.SnapshotMeta, error) {
	meta, err := s.s3TestSource.CheckSnapshotReady(ctx, id)
	if s.readyCalls == 1 {
		return meta, task_errors.NewRetriableErrorf("temporary metadata failure")
	}
	return meta, err
}

func (s flakyS3Source) ReadChunkMap(ctx context.Context, id string, start uint32) (<-chan storage.ChunkMapEntry, <-chan error) {
	if s.mapCalls.Load() == 0 {
		s.mapCalls.Add(1)
		// Partial results must be discarded before retrying the same range.
		return newChunkMapChannels(s.entries, task_errors.NewRetriableErrorf("temporary map failure"))
	}
	return s.s3TestSource.ReadChunkMap(ctx, id, start)
}

func TestS3ExportRetriesSourceAndDestination(t *testing.T) {
	for _, maxRetries := range []int{-1, 1} {
		t.Run(fmt.Sprint(maxRetries), func(t *testing.T) {
			testS3ExportRetriesSourceAndDestination(t, maxRetries)
		})
	}
}

func testS3ExportRetriesSourceAndDestination(t *testing.T, maxRetries int) {
	source := flakyS3Source{&s3TestSource{
		meta:    storage.SnapshotMeta{Size: chunkSize, ChunkCount: 1},
		entries: []storage.ChunkMapEntry{{ChunkIndex: 0, ChunkID: "chunk"}},
	}}
	source.readHook = func(_ context.Context, chunk *common.Chunk) error {
		if source.readCalls.Load() == 1 {
			return context.DeadlineExceeded
		}
		for i := range chunk.Data {
			chunk.Data[i] = 42
		}
		return nil
	}
	destination := newS3TestDestination()
	var attempts int
	destination.put = func(key string, data []byte) error {
		if key != "backup/part-00000001.raw" {
			return nil
		}
		attempts++
		if attempts == 1 {
			return awserr.New(request.CanceledErrorCode, "request context canceled", context.DeadlineExceeded)
		}
		return nil
	}
	options := s3TestOptions()
	options.Prefix = "backup"
	options.Compression = "none"
	options.MaxRetries = maxRetries
	ctx, cancel := context.WithTimeout(newContext(), 5*time.Second)
	defer cancel()
	_, err := ExportToS3(ctx, source, "snapshot", destination, options)
	require.NoError(t, err)
	require.Equal(t, 2, source.readyCalls)
	require.EqualValues(t, 2, source.mapCalls.Load())
	require.EqualValues(t, 2, source.readCalls.Load())
	require.Equal(t, 2, attempts)
	require.Equal(t, bytes.Repeat([]byte{42}, chunkSize), destination.objects["backup/part-00000001.raw"])
	require.Contains(t, destination.objects, "backup/manifest.json")
}

func TestS3ExportRetriesTimeoutAndCanceledReadWorkers(t *testing.T) {
	for _, maxRetries := range []int{-1, 1} {
		t.Run(fmt.Sprint(maxRetries), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(newContext(), 5*time.Second)
			defer cancel()
			// Both initial requests belong to the same failed batch. Retrying
			// uses the still-active export context, not this canceled context.
			batchCtx, cancelBatch := context.WithCancel(ctx)
			defer cancelBatch()
			secondStarted := make(chan struct{})
			var attempts [2]atomic.Int32
			var firstErrors [2]error
			source := &s3TestSource{
				meta: storage.SnapshotMeta{Size: 2 * chunkSize, ChunkCount: 2},
				entries: []storage.ChunkMapEntry{
					{ChunkIndex: 0, ChunkID: "first"},
					{ChunkIndex: 1, ChunkID: "second"},
				},
			}
			source.readHook = func(readCtx context.Context, chunk *common.Chunk) error {
				if err := readCtx.Err(); err != nil {
					return err
				}
				if attempts[chunk.Index].Add(1) == 1 {
					var cause error
					if chunk.Index == 0 {
						// Ensure the second request is in flight before timing out.
						select {
						case <-secondStarted:
						case <-readCtx.Done():
							return readCtx.Err()
						}
						requestCtx, cancelRequest := context.WithDeadline(batchCtx, time.Now())
						<-requestCtx.Done()
						cause = requestCtx.Err()
						cancelRequest()
						cancelBatch()
					} else {
						close(secondStarted)
						<-batchCtx.Done()
						cause = batchCtx.Err()
					}
					// Match the persistence S3 client's wrapping of SDK errors.
					firstErrors[chunk.Index] = task_errors.NewRetriableError(
						awserr.New(request.CanceledErrorCode, "request context canceled", cause),
					)
					return firstErrors[chunk.Index]
				}
				for i := range chunk.Data {
					chunk.Data[i] = chunkDataByte(chunk.Index)
				}
				return nil
			}
			destination := newS3TestDestination()
			options := s3TestOptions()
			options.Compression = "none"
			options.MaxRetries = maxRetries
			manifest, err := ExportToS3(ctx, source, "snapshot", destination, options)
			require.NoError(t, err)
			require.NoError(t, ctx.Err())
			for i, cause := range []error{context.DeadlineExceeded, context.Canceled} {
				var awsError awserr.Error
				require.ErrorAs(t, firstErrors[i], &awsError)
				require.ErrorIs(t, awsError.OrigErr(), cause)
				require.EqualValues(t, 2, attempts[i].Load())
			}
			require.EqualValues(t, 4, source.readCalls.Load())
			require.Zero(t, source.activeReads.Load())
			expected := append(
				bytes.Repeat([]byte{chunkDataByte(0)}, chunkSize),
				bytes.Repeat([]byte{chunkDataByte(1)}, chunkSize)...,
			)
			require.Equal(t, expected, destination.objects["parent/part-00000001.raw"])
			require.Len(t, manifest.Parts, 1)
			require.Contains(t, destination.objects, "parent/manifest.json")
		})
	}
}

func TestS3ExportRetriesTimeoutAndCanceledUploadWorkers(t *testing.T) {
	for _, compression := range []string{"none", "lz4"} {
		for _, maxRetries := range []int{-1, 1} {
			t.Run(fmt.Sprintf("%v/%v", compression, maxRetries), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(newContext(), 5*time.Second)
				defer cancel()
				batchCtx, cancelBatch := context.WithCancel(ctx)
				defer cancelBatch()
				secondStarted := make(chan struct{})
				var attempts [2]atomic.Int32
				var firstErrors [2]error
				var payloads [2][]byte
				source := &s3TestSource{
					meta: storage.SnapshotMeta{Size: 2 * chunkSize, ChunkCount: 2},
					entries: []storage.ChunkMapEntry{
						{ChunkIndex: 0, ChunkID: "first"},
						{ChunkIndex: 1, ChunkID: "second"},
					},
				}
				destination := newS3TestDestination()
				destination.beforePut = func(uploadCtx context.Context, key string, object persistence.S3Object) error {
					if key == "parent/manifest.json" {
						// The completion marker must wait for both retried uploads.
						if attempts[0].Load() != 2 || attempts[1].Load() != 2 {
							return task_errors.NewNonRetriableErrorf("manifest published before upload retries completed")
						}
						return nil
					}
					var index int
					switch key {
					case "parent/" + s3PartKey(1, compression):
						index = 0
					case "parent/" + s3PartKey(2, compression):
						index = 1
					default:
						return task_errors.NewNonRetriableErrorf("unexpected upload key %q", key)
					}
					if attempts[index].Add(1) == 1 {
						payloads[index] = append([]byte(nil), object.Data...)
						var cause error
						if index == 0 {
							select {
							case <-secondStarted:
							case <-uploadCtx.Done():
								return uploadCtx.Err()
							}
							requestCtx, cancelRequest := context.WithDeadline(batchCtx, time.Now())
							<-requestCtx.Done()
							cause = requestCtx.Err()
							cancelRequest()
							cancelBatch()
						} else {
							close(secondStarted)
							<-batchCtx.Done()
							cause = batchCtx.Err()
						}
						firstErrors[index] = task_errors.NewRetriableError(
							awserr.New(request.CanceledErrorCode, "request context canceled", cause),
						)
						return firstErrors[index]
					}
					if !bytes.Equal(payloads[index], object.Data) {
						return task_errors.NewNonRetriableErrorf("part upload payload changed during retry")
					}
					return nil
				}
				options := s3TestOptions()
				options.PartSize = chunkSize
				options.UploadWorkerCount = 2
				options.Compression = compression
				options.MaxRetries = maxRetries
				manifest, err := ExportToS3(ctx, source, "snapshot", destination, options)
				require.NoError(t, err)
				require.NoError(t, ctx.Err())
				for i, cause := range []error{context.DeadlineExceeded, context.Canceled} {
					var awsError awserr.Error
					require.ErrorAs(t, firstErrors[i], &awsError)
					require.ErrorIs(t, awsError.OrigErr(), cause)
					require.EqualValues(t, 2, attempts[i].Load())
					require.Equal(t, payloads[i], destination.objects["parent/"+s3PartKey(uint32(i+1), compression)])
				}
				// Retrying destination requests must not reread the source.
				require.EqualValues(t, 2, source.readCalls.Load())
				require.Len(t, manifest.Parts, 2)
				require.Len(t, destination.uploads, 3)
				require.Equal(t, "parent/manifest.json", destination.uploads[2].key)
			})
		}
	}
}

func TestS3OperationRetryLimit(t *testing.T) {
	for _, tc := range []struct {
		name       string
		maxRetries int
		failures   int
		attempts   int
		wantErr    bool
	}{
		{"disabled", 0, 4, 1, true},
		{"one retry exhausted", 1, 4, 2, true},
		{"two retries exhausted", 2, 4, 3, true},
		{"success on last attempt", 2, 2, 3, false},
		{"unlimited", -1, 4, 5, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(newContext(), 5*time.Second)
			defer cancel()
			attempts := 0
			failure := task_errors.NewRetriableErrorf("temporary failure")
			value, err := retryS3Operation(ctx, tc.maxRetries, func() (int, error) {
				attempts++
				if attempts <= tc.failures {
					return attempts, failure
				}
				return attempts, nil
			})
			if tc.wantErr {
				require.ErrorIs(t, err, failure)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.attempts, attempts)
			require.Equal(t, attempts, value)
		})
	}
}

func TestS3RetryRejectsInvalidLimit(t *testing.T) {
	called := false
	err := retryS3Call(newContext(), -2, func() error {
		called = true
		return nil
	})
	require.ErrorContains(t, err, "S3 max retries")
	require.False(t, called)

	source := &s3TestSource{}
	destination := newS3TestDestination()
	options := s3TestOptions()
	options.MaxRetries = -2
	_, err = ExportToS3(newContext(), source, "snapshot", destination, options)
	require.ErrorContains(t, err, "S3 max retries")
	require.Zero(t, source.readyCalls)
	require.Empty(t, destination.uploads)
}

func TestS3RetryStopsOnPermanentErrorsOrCancellation(t *testing.T) {
	permanent := task_errors.NewNonRetriableErrorf("permanent failure")
	for _, maxRetries := range []int{0, 2, -1} {
		attempts := 0
		err := retryS3Call(newContext(), maxRetries, func() error {
			attempts++
			return permanent
		})
		require.ErrorIs(t, err, permanent)
		require.Equal(t, 1, attempts)
	}

	ctx, cancel := context.WithCancel(newContext())
	attempts := 0
	err := retryS3Call(ctx, -1, func() error {
		attempts++
		cancel()
		return task_errors.NewRetriableErrorf("temporary failure")
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, attempts)

	attempts = 0
	err = retryS3Call(newContext(), 0, func() error {
		attempts++
		return task_errors.NewRetriableErrorf("bounded failure")
	})
	require.Error(t, err)
	require.Equal(t, 1, attempts)
}

func TestS3RetryDoesNotLoopOnManifestDeleteAccessDenied(t *testing.T) {
	destination := newS3TestDestination()
	destination.deleteErr = task_errors.NewRetriableError(awserr.NewRequestFailure(
		awserr.New("AccessDenied", "denied", nil), http.StatusForbidden, "request",
	))
	ctx, cancel := context.WithTimeout(newContext(), time.Second)
	defer cancel()
	err := (retryingS3Destination{S3Destination: destination, maxRetries: -1}).DeleteObject(ctx, "bucket", "manifest.json")
	require.ErrorContains(t, err, "AccessDenied")
	require.False(t, task_errors.CanRetry(err))
}

func TestS3OperationTimeoutClassification(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		retry bool
	}{
		{"deadline", context.DeadlineExceeded, true},
		{"wrapped deadline", fmt.Errorf("reading chunk: %w", context.DeadlineExceeded), true},
		{"network timeout", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, true},
		{"AWS timeout", awserr.New(request.CanceledErrorCode, "request context canceled", context.DeadlineExceeded), true},
		{"AWS cancellation", awserr.New(request.CanceledErrorCode, "request context canceled", context.Canceled), false},
		{"cancellation", context.Canceled, false},
		{"permanent deadline", task_errors.NewNonRetriableError(context.DeadlineExceeded), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.retry, isRetryableS3OperationError(tc.err))
		})
	}
}

func TestS3RetryStopsWhenExportDeadlineExpires(t *testing.T) {
	ctx, cancel := context.WithTimeout(newContext(), 20*time.Millisecond)
	defer cancel()
	attempts := 0
	err := retryS3Call(ctx, -1, func() error {
		attempts++
		<-ctx.Done()
		return awserr.New(request.CanceledErrorCode, "request context canceled", context.DeadlineExceeded)
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, attempts)
}

func TestS3RetryUsesFreshTimeoutAfterAWSRequestCanceled(t *testing.T) {
	ctx, cancel := context.WithTimeout(newContext(), 5*time.Second)
	defer cancel()
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if attempts.Add(1) == 1 {
			select {
			case <-r.Context().Done():
			case <-ctx.Done():
			}
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, err := persistence.NewS3Client(
		server.URL, "region", persistence.S3Credentials{ID: "id", Secret: "secret"},
		200*time.Millisecond, metrics.NewEmptyRegistry(), 0, nil, nil, 0,
	)
	require.NoError(t, err)
	err = (retryingS3Destination{S3Destination: client, maxRetries: -1}).PutObjectWithOptions(
		ctx, "bucket", "part", persistence.S3Object{Data: []byte("payload")},
		persistence.S3PutObjectOptions{},
	)
	require.NoError(t, err)
	require.NoError(t, ctx.Err())
	require.EqualValues(t, 2, attempts.Load())
}
