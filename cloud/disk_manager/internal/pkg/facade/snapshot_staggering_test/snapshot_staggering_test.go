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

// Select test data with enough time to inspect the state before notBefore.
// The implementation itself is tested separately; this helper selects inputs.
func lateSnapshotKey(snapshotID, diskID string) string {
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
			binary.BigEndian.Uint64(digest[:8]) % uint64(10*time.Second),
		)
		if offset >= 7*time.Second {
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

			// Schedule a snapshot whose deadline leaves time for the checks below.
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
				lateSnapshotKey(snapshotID, diskID),
			)

			operation, err = client.CreateSnapshot(reqCtx, request)
			require.NoError(t, err)
			returnedAt := time.Now()

			// The RPC must return before the task starts or creates checkpoints.
			store := testcommon.NewTaskStorage(t, ctx)
			before, err := store.GetTask(ctx, operation.Id)
			require.NoError(t, err)
			require.True(
				t,
				returnedAt.Before(before.AvailableAt),
				"RPC waited for notBefore",
			)
			require.True(t, before.FirstRunStartedAt.IsZero())
			require.GreaterOrEqual(
				t,
				before.AvailableAt.Sub(before.ReceivedAt),
				time.Duration(0),
			)
			require.LessOrEqual(
				t,
				before.AvailableAt.Sub(before.ReceivedAt),
				10*time.Second+time.Microsecond,
			)
			testcommon.RequireCheckpointsDoNotExist(t, ctx, diskID)

			// An idempotent repeat must preserve both the operation and its deadline.
			repeated, err := client.CreateSnapshot(reqCtx, request)
			require.NoError(t, err)
			require.Equal(t, operation.Id, repeated.Id)

			repeatedState, err := store.GetTask(ctx, operation.Id)
			require.NoError(t, err)
			require.True(t, before.AvailableAt.Equal(repeatedState.AvailableAt))

			if cancelBeforeStart {
				testcommon.CancelOperation(t, ctx, client, operation.Id)

				remaining := time.Until(before.AvailableAt)
				require.Greater(
					t,
					remaining,
					time.Duration(0),
					"cancel RPC waited for notBefore",
				)

				testcommon.WaitOperationEnded(t, ctx, operation.Id, remaining)
				require.True(
					t,
					time.Now().Before(before.AvailableAt),
					"cancellation did not finish before notBefore",
				)

				after, err := store.GetTask(ctx, operation.Id)
				require.NoError(t, err)
				require.Equal(t, storage.TaskStatusCancelled, after.Status)
				require.True(t, after.FirstRunStartedAt.IsZero())
				testcommon.RequireCheckpointsDoNotExist(t, ctx, diskID)
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
				require.False(t, after.FirstRunStartedAt.Before(before.AvailableAt))
				require.Equal(t, int64(128*1024*1024), response.Size)
			}
		})
	}
}
