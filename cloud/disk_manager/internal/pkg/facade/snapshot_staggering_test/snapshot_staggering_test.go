package tests

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

// Keep in sync with --snapshot-staggering-window in ya.make
const staggeringWindow = 5 * time.Minute

// The implementation itself is tested separately; this helper selects inputs.
func snapshotKey(snapshotID, diskID string, cancelBeforeStart bool) string {
	minOffset, maxOffset := 7*time.Second, 10*time.Second
	if cancelBeforeStart {
		minOffset, maxOffset = 4*time.Minute, staggeringWindow
	}

	for i := 0; ; i++ {
		key := fmt.Sprintf("%s-%d", snapshotID, i)

		var data []byte
		var length [8]byte

		for _, value := range []string{key, snapshotID, "zone-a", diskID} {
			binary.BigEndian.PutUint64(length[:], uint64(len(value)))

			data = append(data, length[:]...)
			data = append(data, value...)
		}

		digest := sha256.Sum256(data)
		offset := time.Duration(
			binary.BigEndian.Uint64(digest[:8]) % uint64(staggeringWindow),
		)
		if offset >= minOffset && offset <= maxOffset {
			return key
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestSnapshotStaggeringFacade(t *testing.T) {
	for _, cancelBeforeStart := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%v", cancelBeforeStart), func(t *testing.T) {
			ctx := testcommon.NewContext()

			client, err := testcommon.NewClient(ctx)
			require.NoError(t, err)
			defer client.Close()

			// Create the source disk before checking snapshot scheduling.
			diskID := strings.NewReplacer(
				"/", "-",
				"=", "-",
			).Replace(t.Name())

			operation, err := client.CreateDisk(
				testcommon.GetRequestContext(t, ctx),
				&disk_manager.CreateDiskRequest{
					Src: &disk_manager.CreateDiskRequest_SrcEmpty{
						SrcEmpty: &empty.Empty{},
					},
					Size:      128 * 1024 * 1024,
					BlockSize: 4096,
					Kind:      disk_manager.DiskKind_DISK_KIND_SSD,
					DiskId: &disk_manager.DiskId{
						ZoneId: "zone-a",
						DiskId: diskID,
					},
				},
			)

			require.NoError(t, err)

			diskWaitCtx, cancelDiskWait := context.WithTimeout(ctx, time.Minute)
			defer cancelDiskWait()

			require.NoError(
				t,
				internal_client.WaitOperation(diskWaitCtx, client, operation.Id),
				"waiting for source disk operation %s", operation.Id,
			)

			// Prepare and warm up observation clients before starting the delay.
			store := testcommon.NewTaskStorage(t, ctx)
			_, err = store.GetTask(ctx, operation.Id)
			require.NoError(t, err)

			nbsClient := testcommon.NewNbsTestingClient(t, ctx, "zone-a")
			checkpoints, err := nbsClient.GetCheckpoints(ctx, diskID)
			require.NoError(t, err)
			require.Empty(t, checkpoints)

			// Keep the success path short and reserve a long delay for cancellation.
			snapshotID := diskID + "-snapshot"
			request := &disk_manager.CreateSnapshotRequest{
				Src: &disk_manager.DiskId{
					ZoneId: "zone-a",
					DiskId: diskID,
				},
				SnapshotId: snapshotID,
				FolderId:   "folder",
			}
			reqCtx := headers.SetOutgoingIdempotencyKey(
				ctx,
				snapshotKey(snapshotID, diskID, cancelBeforeStart),
			)

			operation, err = client.CreateSnapshot(reqCtx, request)
			require.NoError(t, err)
			returnedAt := time.Now()

			before, err := store.GetTask(ctx, operation.Id)
			stateObservedAt := time.Now()
			require.NoError(t, err)

			if cancelBeforeStart {
				// Check asynchronous submission using the long cancellation delay.
				require.True(
					t,
					returnedAt.Before(before.AvailableAt),
					"snapshot RPC did not return before notBefore",
				)
			}

			if stateObservedAt.Before(before.AvailableAt) {
				require.True(t, before.FirstRunStartedAt.IsZero())
			}

			require.GreaterOrEqual(
				t,
				before.AvailableAt.Sub(before.ReceivedAt),
				time.Duration(0),
			)
			require.LessOrEqual(
				t,
				before.AvailableAt.Sub(before.ReceivedAt),
				staggeringWindow+time.Microsecond,
			)

			checkpoints, err = nbsClient.GetCheckpoints(ctx, diskID)
			checkpointsObservedAt := time.Now()
			require.NoError(t, err)

			if checkpointsObservedAt.Before(before.AvailableAt) {
				require.Empty(t, checkpoints)
			}

			// An idempotent repeat must preserve both the operation and its deadline.
			repeated, err := client.CreateSnapshot(reqCtx, request)
			require.NoError(t, err)
			require.Equal(t, operation.Id, repeated.Id)

			repeatedState, err := store.GetTask(ctx, operation.Id)
			require.NoError(t, err)
			require.True(t, before.AvailableAt.Equal(repeatedState.AvailableAt))

			if cancelBeforeStart {
				cancelCtx, cancelWait := context.WithTimeout(ctx, time.Minute)
				defer cancelWait()
				testcommon.CancelOperation(t, cancelCtx, client, operation.Id)

				// Reuse storage instead of creating a scheduler and another YDB client.
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()

				var after storage.TaskState
				for {
					after, err = store.GetTask(cancelCtx, operation.Id)
					require.NoError(t, err, "waiting for cancellation of %s", operation.Id)

					if storage.IsEnded(after.Status) {
						break
					}

					select {
					case <-cancelCtx.Done():
						require.NoError(t, cancelCtx.Err(), "waiting for cancellation of %s", operation.Id)
					case <-ticker.C:
					}
				}

				require.Equal(t, storage.TaskStatusCancelled, after.Status)
				require.True(t, after.FirstRunStartedAt.IsZero())
				require.False(t, after.EndedAt.IsZero())
				require.True(
					t,
					after.EndedAt.Before(before.AvailableAt),
					"cancellation did not finish before notBefore",
				)

				checkpoints, err = nbsClient.GetCheckpoints(ctx, diskID)
				require.NoError(t, err)
				require.Empty(t, checkpoints)
			} else {
				snapshotWaitCtx, cancelSnapshotWait := context.WithTimeout(ctx, time.Minute)
				defer cancelSnapshotWait()

				response := &disk_manager.CreateSnapshotResponse{}
				require.NoError(
					t,
					internal_client.WaitResponse(
						snapshotWaitCtx,
						client,
						operation.Id,
						response,
					),
					"waiting for snapshot operation %s", operation.Id,
				)

				after, err := store.GetTask(ctx, operation.Id)
				require.NoError(t, err)
				require.False(t, after.FirstRunStartedAt.IsZero())
				require.False(t, after.FirstRunStartedAt.Before(before.AvailableAt))
				require.Equal(t, int64(128*1024*1024), response.Size)
			}
		})
	}
}
