package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

const (
	// The checkpoint of the base snapshot does not exist on the disk. The next
	// snapshot of the disk can't be created: its changed blocks are computed
	// against this checkpoint.
	incrementalProblemBaseCheckpointNotFound = "baseCheckpointNotFound"
	// The disk has several heads of the incremental chain, so the base of the
	// next snapshot is picked among them by snapshot ID.
	incrementalProblemMultipleHeads = "multipleHeads"
	// The disk has several heads of the incremental chain and the next snapshot
	// is based not on the latest of them.
	incrementalProblemBaseSnapshotIsNotLatest = "baseSnapshotIsNotLatest"
	// The base snapshot does not exist or is not ready. The next snapshot of
	// the disk is full.
	incrementalProblemBaseSnapshotNotReady = "baseSnapshotNotReady"
	// The disk does not exist, but still has an incremental chain. Snapshots of
	// a new disk with the same ID will be based on this chain.
	incrementalProblemDiskNotFound = "diskNotFound"

	// Rows of the incremental table read at a time. Should not exceed the row
	// limit of a YDB query result.
	checkIncrementalSnapshotsPageSize = 1000

	maxDisksWithProblemsInState = 100

	maxFailedZoneRequestsInARow = 3
)

var incrementalProblems = []string{
	incrementalProblemBaseCheckpointNotFound,
	incrementalProblemMultipleHeads,
	incrementalProblemBaseSnapshotIsNotLatest,
	incrementalProblemBaseSnapshotNotReady,
	incrementalProblemDiskNotFound,
}

////////////////////////////////////////////////////////////////////////////////

// Checks, for every disk that has an incremental chain, that the next snapshot
// of the disk can be created.
//
// The next snapshot of a disk is based on the head of its incremental chain: a
// row of the incremental table. The checkpoint of the head should be present
// on the disk, otherwise the changed blocks can't be computed and the snapshot
// creation fails. Normally the head is the latest snapshot of the disk and its
// checkpoint is the only one kept on the disk.
//
// Disks without rows in the incremental table are not checked: their next
// snapshot is full and does not depend on the previous ones.
//
// The task only reports the problems: to the log, to the metrics and to its
// state.
type checkIncrementalSnapshotsTask struct {
	storage       storage.Storage
	nbsFactory    nbs.Factory
	disabledZones *common.DisabledZones
	registry      metrics.Registry
	pageSize      int
	state         *protos.CheckIncrementalSnapshotsTaskState

	// The zone of the last failed check and the number of checks that have
	// failed in a row there.
	failedZoneID       string
	failedRequestCount int
}

func (t *checkIncrementalSnapshotsTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *checkIncrementalSnapshotsTask) Load(_, state []byte) error {
	t.state = &protos.CheckIncrementalSnapshotsTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *checkIncrementalSnapshotsTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	for {
		entries, err := t.storage.ListIncremental(
			ctx,
			t.state.LastCheckedDisk,
			t.pageSize,
		)
		if err != nil {
			return err
		}

		lastPage := len(entries) < t.pageSize

		disks := groupIncrementalEntriesByDisk(entries)
		if !lastPage && len(disks) > 1 {
			// The rest of the rows of the last disk may be on the next page.
			disks = disks[:len(disks)-1]
		}

		for _, diskEntries := range disks {
			err := t.checkDisk(ctx, diskEntries)
			if err != nil {
				return err
			}

			t.state.LastCheckedDisk = diskEntries[0].Disk
		}

		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}

		if lastPage {
			break
		}
	}

	logging.Info(
		ctx,
		"Checked incremental snapshots of %v disks, skipped %v disks, problems: %v",
		t.state.CheckedDiskCount,
		t.state.SkippedDiskCount,
		t.state.ProblemCounts,
	)
	t.reportMetrics()

	return nil
}

func (t *checkIncrementalSnapshotsTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *checkIncrementalSnapshotsTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *checkIncrementalSnapshotsTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

// Splits rows ordered by disk into the groups of rows of the same disk.
func groupIncrementalEntriesByDisk(
	entries []storage.IncrementalEntry,
) [][]storage.IncrementalEntry {

	var disks [][]storage.IncrementalEntry

	start := 0
	for i := 1; i <= len(entries); i++ {
		if i == len(entries) ||
			entries[i].Disk.ZoneId != entries[start].Disk.ZoneId ||
			entries[i].Disk.DiskId != entries[start].Disk.DiskId {

			disks = append(disks, entries[start:i])
			start = i
		}
	}

	return disks
}

// |entries| are the heads of the incremental chain of a disk.
func (t *checkIncrementalSnapshotsTask) checkDisk(
	ctx context.Context,
	entries []storage.IncrementalEntry,
) error {

	disk := entries[0].Disk
	base := entries[0]
	var problems []string

	if len(entries) > 1 {
		// Find out which of the heads the next snapshot is going to be based
		// on.
		baseSnapshotID, _, err := t.storage.GetIncremental(ctx, disk)
		if err != nil {
			return err
		}

		latest := entries[0]
		for _, entry := range entries {
			if entry.SnapshotID == baseSnapshotID {
				base = entry
			}

			if entry.CreatedAt.After(latest.CreatedAt) {
				latest = entry
			}
		}

		problems = append(problems, incrementalProblemMultipleHeads)
		if base.SnapshotID != latest.SnapshotID {
			problems = append(
				problems,
				incrementalProblemBaseSnapshotIsNotLatest,
			)
		}
	}

	snapshotMeta, err := t.storage.GetSnapshotMeta(ctx, base.SnapshotID)
	if err != nil {
		return err
	}

	t.state.CheckedDiskCount++

	if snapshotMeta == nil || !snapshotMeta.Ready {
		// Such a snapshot can't be locked, so it is not used as a base and its
		// checkpoint is not needed.
		problems = append(problems, incrementalProblemBaseSnapshotNotReady)
	} else if !t.isZoneAvailable(disk.ZoneId) {
		t.state.SkippedDiskCount++
	} else {
		problem, err := t.checkBaseCheckpoint(ctx, base)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			logging.Warn(
				ctx,
				"Failed to check checkpoint %v of disk %+v: %v",
				base.CheckpointID,
				disk,
				err,
			)
			t.zoneRequestFailed(ctx, disk.ZoneId)
			t.state.SkippedDiskCount++
		} else {
			t.failedRequestCount = 0
			if len(problem) != 0 {
				problems = append(problems, problem)
			}
		}
	}

	if len(problems) == 0 {
		return nil
	}

	// The chain is not read atomically with the state of its base snapshot and
	// of the disk. Checkpoint of a snapshot is deleted only after the snapshot
	// is removed from the incremental table, so the problems are real only if
	// the base is still the same.
	baseSnapshotID, _, err := t.storage.GetIncremental(ctx, disk)
	if err != nil {
		return err
	}

	if baseSnapshotID != base.SnapshotID {
		logging.Info(
			ctx,
			"Incremental chain of disk %+v has changed during the check",
			disk,
		)
		return nil
	}

	t.reportProblems(ctx, base, problems)
	return nil
}

// Checks that the checkpoint of the base snapshot exists on the disk. Returns
// the problem found, if any. Returns error if the check is impossible.
func (t *checkIncrementalSnapshotsTask) checkBaseCheckpoint(
	ctx context.Context,
	base storage.IncrementalEntry,
) (string, error) {

	disk := base.Disk

	nbsClient, err := t.nbsFactory.GetClient(ctx, disk.ZoneId)
	if err != nil {
		return "", err
	}

	checkpoints, err := nbsClient.GetCheckpoints(ctx, disk.DiskId)
	if err != nil {
		if nbs.IsNotFoundError(err) {
			return incrementalProblemDiskNotFound, nil
		}

		return "", err
	}

	if common.Find(checkpoints, base.CheckpointID) {
		return "", nil
	}

	diskParams, err := nbsClient.Describe(ctx, disk.DiskId)
	if err != nil {
		if nbs.IsNotFoundError(err) {
			return incrementalProblemDiskNotFound, nil
		}

		return "", err
	}

	if diskParams.IsDiskRegistryBasedDisk {
		// Snapshots of such disks are always full and their checkpoints are
		// deleted right after the snapshot is created.
		return "", nil
	}

	return incrementalProblemBaseCheckpointNotFound, nil
}

// Disks are checked zone by zone. NBS requests to an unavailable zone fail only
// after a long timeout, so after several failures in a row the rest of the
// disks of the zone are skipped.
func (t *checkIncrementalSnapshotsTask) isZoneAvailable(zoneID string) bool {
	if t.disabledZones.IsDisabled(zoneID) {
		return false
	}

	return t.failedZoneID != zoneID ||
		t.failedRequestCount < maxFailedZoneRequestsInARow
}

func (t *checkIncrementalSnapshotsTask) zoneRequestFailed(
	ctx context.Context,
	zoneID string,
) {

	if t.failedZoneID != zoneID {
		t.failedZoneID = zoneID
		t.failedRequestCount = 0
	}

	t.failedRequestCount++
	if t.failedRequestCount == maxFailedZoneRequestsInARow {
		logging.Warn(
			ctx,
			"Skipping the rest of the disks of zone %v: %v checks in a row have failed",
			zoneID,
			t.failedRequestCount,
		)
	}
}

func (t *checkIncrementalSnapshotsTask) reportProblems(
	ctx context.Context,
	base storage.IncrementalEntry,
	problems []string,
) {

	if t.state.ProblemCounts == nil {
		t.state.ProblemCounts = make(map[string]uint64)
	}

	for _, problem := range problems {
		t.state.ProblemCounts[problem]++
	}

	if len(t.state.DisksWithProblems) < maxDisksWithProblemsInState {
		t.state.DisksWithProblems = append(
			t.state.DisksWithProblems,
			&protos.DiskIncrementalSnapshotsProblems{
				Disk:             base.Disk,
				BaseSnapshotId:   base.SnapshotID,
				BaseCheckpointId: base.CheckpointID,
				Problems:         problems,
			},
		)
	}

	if common.Find(problems, incrementalProblemBaseCheckpointNotFound) {
		logging.Error(
			ctx,
			"Snapshot of disk %+v can't be created: checkpoint %v of base snapshot %v does not exist, problems: %v",
			base.Disk,
			base.CheckpointID,
			base.SnapshotID,
			problems,
		)
		return
	}

	logging.Warn(
		ctx,
		"Incremental chain of disk %+v with base snapshot %v and checkpoint %v has problems: %v",
		base.Disk,
		base.SnapshotID,
		base.CheckpointID,
		problems,
	)
}

// Counters are used instead of gauges because every run of the task may be
// executed on a different host, and a host keeps the gauge values of its last
// run.
func (t *checkIncrementalSnapshotsTask) reportMetrics() {
	t.registry.Counter("incremental/checkedDisks").Add(
		int64(t.state.CheckedDiskCount),
	)
	t.registry.Counter("incremental/skippedDisks").Add(
		int64(t.state.SkippedDiskCount),
	)

	for _, problem := range incrementalProblems {
		t.registry.WithTags(map[string]string{
			"problem": problem,
		}).Counter("incremental/disksWithProblems").Add(
			int64(t.state.ProblemCounts[problem]),
		)
	}
}
