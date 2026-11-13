package dataplane

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/golang/protobuf/proto"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

func TestExportSourceObjectErrorsAreTerminal(t *testing.T) {
	for _, code := range []string{"InternalError", "NoSuchKey", "NotFound"} {
		t.Run(code, func(t *testing.T) {
			retries := newSnapshotExportSourceRetries(
				&protos.ExportSnapshotTaskState{}, 30*time.Minute,
			)
			err := retries.read(
				context.Background(), "object", func(context.Context) error {
					return fmt.Errorf("read: %w", errors.NewRetriableError(
						awserr.NewRequestFailure(awserr.New(code, "", nil), 500, ""),
					))
				},
			)
			var sourceError *snapshotExportSourceError
			if !errors.As(err, &sourceError) || sourceError.retryable {
				t.Fatalf("object error must be terminal: %v", err)
			}
			if !retries.startedAt.IsZero() {
				t.Fatal("terminal error must not open a retry window")
			}
		})
	}
}

func TestExportSourceRetryWindowSurvivesCheckpoint(t *testing.T) {
	now := time.Now()
	state := &protos.ExportSnapshotTaskState{}
	retries := newSnapshotExportSourceRetries(state, 30*time.Minute)
	retries.now = func() time.Time { return now }
	err := retries.read(
		context.Background(), "failed", func(context.Context) error {
			return io.ErrUnexpectedEOF
		},
	)
	var sourceError *snapshotExportSourceError
	if !errors.As(err, &sourceError) || !sourceError.retryable {
		t.Fatalf("EOF must open a retry window: %v", err)
	}

	if errors.CanRetry(err) {
		t.Fatal("the exporter must not apply destination retries to source reads")
	}

	retries.checkpoint(state)
	data, err := proto.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}

	restored := &protos.ExportSnapshotTaskState{}
	if err := proto.Unmarshal(data, restored); err != nil {
		t.Fatal(err)
	}

	retries = newSnapshotExportSourceRetries(restored, 30*time.Minute)
	retries.now = func() time.Time { return now }
	if err := retries.read(
		context.Background(), "other", func(context.Context) error { return nil },
	); err != nil {
		t.Fatal(err)
	}

	now = now.Add(30 * time.Minute)
	if !retries.expired() {
		t.Fatal("unrelated success must not extend the original retry window")
	}

	now = now.Add(-time.Minute)
	if err := retries.read(
		context.Background(), "failed", func(context.Context) error { return nil },
	); err != nil {
		t.Fatal(err)
	}

	retries.checkpoint(state)
	if state.SourceRetryStartedAt != 0 || state.SourceRetryObject != "" {
		t.Fatal("successful recovery must clear the durable retry window")
	}
}

func TestExportSourceTimeoutsHaveNoRetryWindow(t *testing.T) {
	for _, failure := range []error{
		context.DeadlineExceeded,
		&net.DNSError{IsTimeout: true},
		awserr.New("RequestCanceled", "", context.DeadlineExceeded),
		awserr.New("RequestTimeout", "", nil),
		awserr.New("RequestTimeoutException", "", nil),
		awserr.NewRequestFailure(
			awserr.New("Timeout", "", nil), http.StatusRequestTimeout, "",
		),
		awserr.NewRequestFailure(
			awserr.New("Timeout", "", nil), http.StatusGatewayTimeout, "",
		),
		awserr.NewRequestFailure(
			awserr.New("InternalError", "", nil), http.StatusGatewayTimeout, "",
		),
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			now := time.Now()
			state := &protos.ExportSnapshotTaskState{
				SourceRetryStartedAt: now.Add(-time.Hour).UnixNano(),
				SourceRetryObject:    "failed",
			}
			for attempt := 0; attempt < 3; attempt++ {
				retries := newSnapshotExportSourceRetries(state, 30*time.Minute)
				retries.now = func() time.Time { return now }
				err := retries.read(
					context.Background(), "failed", func(ctx context.Context) error {
						if _, ok := ctx.Deadline(); ok {
							t.Fatal("old retry window must not limit a new read")
						}

						return errors.NewRetriableError(failure)
					},
				)
				var sourceError *snapshotExportSourceError
				if !errors.As(err, &sourceError) ||
					!sourceError.retryable || !sourceError.timedOut {

					t.Fatalf("timeout must yield the task: %v", err)
				}

				retries.checkpoint(state)
				if state.SourceRetryStartedAt != 0 || retries.expired() {
					t.Fatal("timeout must clear the bounded retry window")
				}

				now = now.Add(time.Hour)
			}
		})
	}
}

func TestExportSourceParentCancellationIsNotRetried(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	retries := newSnapshotExportSourceRetries(
		&protos.ExportSnapshotTaskState{}, 30*time.Minute,
	)
	err := retries.read(ctx, "failed", func(context.Context) error {
		cancel()
		return context.DeadlineExceeded
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("parent cancellation must stop the read: %v", err)
	}
}

func TestExportTargetAuthenticationErrorIsTerminal(t *testing.T) {
	err := wrapSnapshotExportTargetError(
		errors.NewRetriableError(awserr.NewRequestFailure(
			awserr.New("AccessDenied", "", nil), 403, "",
		)),
		"PutObject", "bucket", "key",
	)
	if errors.CanRetry(err) {
		t.Fatal("authentication errors must not be retried")
	}
}

func TestExportUploadConcurrencyRecoversWithinConfiguredLimit(t *testing.T) {
	state := &protos.ExportSnapshotTaskState{}
	limiter := newSnapshotExportUploadLimiter(
		10, state, metrics.NewEmptyRegistry(),
	)
	defer limiter.close()
	ctx := newContext()
	for _, expected := range []uint32{5, 2, 1} {
		generation, err := limiter.acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		limiter.finish(ctx, generation, context.DeadlineExceeded)
		limiter.checkpoint(state)
		if state.UploadWorkerLimit != expected {
			t.Fatalf("expected %v workers, got %v", expected, state.UploadWorkerLimit)
		}
	}

	for i := 0; i < 100; i++ {
		generation, err := limiter.acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		limiter.finish(ctx, generation, nil)
	}
	limiter.checkpoint(state)
	if state.UploadWorkerLimit != 10 {
		t.Fatalf(
			"concurrency must recover to its maximum: %v", state.UploadWorkerLimit,
		)
	}
}

func TestExportUploadRecoveryIgnoresRequestsStartedBeforeFailure(t *testing.T) {
	state := &protos.ExportSnapshotTaskState{}
	limiter := newSnapshotExportUploadLimiter(
		10, state, metrics.NewEmptyRegistry(),
	)
	defer limiter.close()
	ctx := newContext()
	var generations []uint64
	for i := 0; i < 10; i++ {
		generation, err := limiter.acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		generations = append(generations, generation)
	}
	limiter.finish(ctx, generations[0], context.DeadlineExceeded)
	for _, generation := range generations[1:] {
		limiter.finish(ctx, generation, nil)
	}
	limiter.checkpoint(state)
	if state.UploadWorkerLimit != 5 {
		t.Fatal("successes from the failed batch must not restore concurrency")
	}
}
