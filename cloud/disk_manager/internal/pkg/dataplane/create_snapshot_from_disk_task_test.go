package dataplane

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
)

////////////////////////////////////////////////////////////////////////////////

// The batched transfer resume relies on the task persisting the milestone
// (including CurrentBatchBitmap) into its state and restoring it on the next
// run. Check that this round-trip keeps every field, so a rescheduled task
// redoes only the unwritten chunks of the current batch.
func TestCreateSnapshotFromDiskTransferMilestoneRoundTrip(t *testing.T) {
	task := createSnapshotFromDiskTask{
		state: &protos.CreateSnapshotFromDiskTaskState{},
	}

	milestone := common.Milestone{
		ChunkIndex:            42,
		TransferredChunkCount: 40,
		CurrentBatchBitmap:    []byte{0xb1, 0x00, 0xff, 0x0c},
	}

	task.applyTransferMilestone(milestone)

	require.EqualValues(t, 42, task.state.MilestoneChunkIndex)
	require.EqualValues(t, 40, task.state.TransferredChunkCount)
	require.Equal(t, milestone.CurrentBatchBitmap, task.state.CurrentBatchBitmap)

	// The milestone restored for a resumed transfer must equal the one that was
	// persisted, bitmap included.
	require.Equal(t, milestone, task.transferMilestone())
}
