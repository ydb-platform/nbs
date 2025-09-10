package util

import (
	"encoding/json"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	disk_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	filesystem_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem/protos"
	images_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	placementgroup_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/placementgroup/protos"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	snapshot_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
	grpc_codes "google.golang.org/grpc/codes"
)

////////////////////////////////////////////////////////////////////////////////

var requestProtoByTaskType = map[string]func() proto.Message{
	"dataplane.CreateSnapshotFromDisk":              func() proto.Message { return &dataplane_protos.CreateSnapshotFromDiskRequest{} },
	"dataplane.CreateSnapshotFromSnapshot":          func() proto.Message { return &dataplane_protos.CreateSnapshotFromSnapshotRequest{} },
	"dataplane.CreateSnapshotFromLegacySnapshot":    func() proto.Message { return &dataplane_protos.CreateSnapshotFromLegacySnapshotRequest{} },
	"dataplane.CreateSnapshotFromURL":               func() proto.Message { return &dataplane_protos.CreateSnapshotFromURLRequest{} },
	"dataplane.TransferFromDiskToDisk":              func() proto.Message { return &dataplane_protos.TransferFromDiskToDiskRequest{} },
	"dataplane.ReplicateDisk":                       func() proto.Message { return &dataplane_protos.ReplicateDiskTaskRequest{} },
	"dataplane.TransferFromSnapshotToDisk":          func() proto.Message { return &dataplane_protos.TransferFromSnapshotToDiskRequest{} },
	"dataplane.TransferFromLegacySnapshotToDisk":    func() proto.Message { return &dataplane_protos.TransferFromSnapshotToDiskRequest{} },
	"dataplane.DeleteSnapshot":                      func() proto.Message { return &dataplane_protos.DeleteSnapshotRequest{} },
	"dataplane.DeleteSnapshotData":                  func() proto.Message { return &dataplane_protos.DeleteSnapshotDataRequest{} },
	"dataplane.DeleteDiskFromIncremental":           func() proto.Message { return &dataplane_protos.DeleteDiskFromIncrementalRequest{} },
	"dataplane.CreateDRBasedDiskCheckpoint":         func() proto.Message { return &dataplane_protos.CreateDRBasedDiskCheckpointRequest{} },
	"disks.CreateEmptyDisk":                         func() proto.Message { return &disk_protos.CreateDiskParams{} },
	"disks.CreateOverlayDisk":                       func() proto.Message { return &disk_protos.CreateOverlayDiskRequest{} },
	"disks.CreateDiskFromImage":                     func() proto.Message { return &disk_protos.CreateDiskFromImageRequest{} },
	"disks.CreateDiskFromSnapshot":                  func() proto.Message { return &disk_protos.CreateDiskFromSnapshotRequest{} },
	"disks.DeleteDisk":                              func() proto.Message { return &disk_protos.DeleteDiskRequest{} },
	"disks.ResizeDisk":                              func() proto.Message { return &disk_protos.ResizeDiskRequest{} },
	"disks.AlterDisk":                               func() proto.Message { return &disk_protos.AlterDiskRequest{} },
	"disks.AssignDisk":                              func() proto.Message { return &disk_protos.AssignDiskRequest{} },
	"disks.UnassignDisk":                            func() proto.Message { return &disk_protos.UnassignDiskRequest{} },
	"disks.MigrateDisk":                             func() proto.Message { return &disk_protos.MigrateDiskRequest{} },
	"filesystem.CreateFilesystem":                   func() proto.Message { return &filesystem_protos.CreateFilesystemRequest{} },
	"filesystem.CreateExternalFilesystem":           func() proto.Message { return &filesystem_protos.CreateFilesystemRequest{} },
	"filesystem.DeleteFilesystem":                   func() proto.Message { return &filesystem_protos.DeleteFilesystemRequest{} },
	"filesystem.DeleteExternalFilesystem":           func() proto.Message { return &filesystem_protos.DeleteFilesystemRequest{} },
	"filesystem.ResizeFilesystem":                   func() proto.Message { return &filesystem_protos.ResizeFilesystemRequest{} },
	"images.CreateImageFromURL":                     func() proto.Message { return &images_protos.CreateImageFromURLRequest{} },
	"images.CreateImageFromImage":                   func() proto.Message { return &images_protos.CreateImageFromImageRequest{} },
	"images.CreateImageFromSnapshot":                func() proto.Message { return &images_protos.CreateImageFromSnapshotRequest{} },
	"images.CreateImageFromDisk":                    func() proto.Message { return &images_protos.CreateImageFromDiskRequest{} },
	"images.DeleteImage":                            func() proto.Message { return &images_protos.DeleteImageRequest{} },
	"placement_group.CreatePlacementGroup":          func() proto.Message { return &placementgroup_protos.CreatePlacementGroupRequest{} },
	"placement_group.DeletePlacementGroup":          func() proto.Message { return &placementgroup_protos.DeletePlacementGroupRequest{} },
	"placement_group.AlterPlacementGroupMembership": func() proto.Message { return &placementgroup_protos.AlterPlacementGroupMembershipRequest{} },
	"pools.AcquireBaseDisk":                         func() proto.Message { return &pools_protos.AcquireBaseDiskRequest{} },
	"pools.CreateBaseDisk":                          func() proto.Message { return &pools_protos.CreateBaseDiskRequest{} },
	"pools.ReleaseBaseDisk":                         func() proto.Message { return &pools_protos.ReleaseBaseDiskRequest{} },
	"pools.RebaseOverlayDisk":                       func() proto.Message { return &pools_protos.RebaseOverlayDiskRequest{} },
	"pools.ConfigurePool":                           func() proto.Message { return &pools_protos.ConfigurePoolRequest{} },
	"pools.DeletePool":                              func() proto.Message { return &pools_protos.DeletePoolRequest{} },
	"pools.ImageDeleting":                           func() proto.Message { return &pools_protos.ImageDeletingRequest{} },
	"pools.RetireBaseDisk":                          func() proto.Message { return &pools_protos.RetireBaseDiskRequest{} },
	"pools.RetireBaseDisks":                         func() proto.Message { return &pools_protos.RetireBaseDisksRequest{} },
	"snapshots.CreateSnapshotFromDisk":              func() proto.Message { return &snapshot_protos.CreateSnapshotFromDiskRequest{} },
	"snapshots.DeleteSnapshot":                      func() proto.Message { return &snapshot_protos.DeleteSnapshotRequest{} },
}

var stateProtoByTaskType = map[string]func() proto.Message{
	"dataplane.CollectSnapshots":                    func() proto.Message { return &dataplane_protos.CollectSnapshotsTaskState{} },
	"dataplane.CreateSnapshotFromDisk":              func() proto.Message { return &dataplane_protos.CreateSnapshotFromDiskTaskState{} },
	"dataplane.CreateSnapshotFromSnapshot":          func() proto.Message { return &dataplane_protos.CreateSnapshotFromSnapshotTaskState{} },
	"dataplane.CreateSnapshotFromLegacySnapshot":    func() proto.Message { return &dataplane_protos.CreateSnapshotFromLegacySnapshotTaskState{} },
	"dataplane.CreateSnapshotFromURL":               func() proto.Message { return &dataplane_protos.CreateSnapshotFromURLTaskState{} },
	"dataplane.TransferFromDiskToDisk":              func() proto.Message { return &dataplane_protos.TransferFromDiskToDiskTaskState{} },
	"dataplane.ReplicateDisk":                       func() proto.Message { return &dataplane_protos.ReplicateDiskTaskState{} },
	"dataplane.TransferFromSnapshotToDisk":          func() proto.Message { return &dataplane_protos.TransferFromSnapshotToDiskTaskState{} },
	"dataplane.TransferFromLegacySnapshotToDisk":    func() proto.Message { return &dataplane_protos.TransferFromSnapshotToDiskTaskState{} },
	"dataplane.DeleteSnapshot":                      func() proto.Message { return &dataplane_protos.DeleteSnapshotTaskState{} },
	"dataplane.DeleteSnapshotData":                  func() proto.Message { return &dataplane_protos.DeleteSnapshotDataTaskState{} },
	"dataplane.DeleteDiskFromIncremental":           func() proto.Message { return &dataplane_protos.DeleteDiskFromIncrementalState{} },
	"dataplane.CreateDRBasedDiskCheckpoint":         func() proto.Message { return &dataplane_protos.CreateDRBasedDiskCheckpointTaskState{} },
	"dataplane.MigrateSnapshotTask":                 func() proto.Message { return &dataplane_protos.MigrateSnapshotTaskState{} },
	"dataplane.MigrateSnapshotDatabaseTask":         func() proto.Message { return &dataplane_protos.MigrateSnapshotDatabaseTaskState{} },
	"disks.CreateEmptyDisk":                         func() proto.Message { return &disk_protos.CreateEmptyDiskTaskState{} },
	"disks.CreateOverlayDisk":                       func() proto.Message { return &disk_protos.CreateOverlayDiskTaskState{} },
	"disks.CreateDiskFromImage":                     func() proto.Message { return &disk_protos.CreateDiskFromImageTaskState{} },
	"disks.CreateDiskFromSnapshot":                  func() proto.Message { return &disk_protos.CreateDiskFromSnapshotTaskState{} },
	"disks.DeleteDisk":                              func() proto.Message { return &disk_protos.DeleteDiskTaskState{} },
	"disks.ResizeDisk":                              func() proto.Message { return &disk_protos.ResizeDiskTaskState{} },
	"disks.AlterDisk":                               func() proto.Message { return &disk_protos.AlterDiskTaskState{} },
	"disks.AssignDisk":                              func() proto.Message { return &disk_protos.AssignDiskTaskState{} },
	"disks.UnassignDisk":                            func() proto.Message { return &disk_protos.UnassignDiskTaskState{} },
	"disks.MigrateDisk":                             func() proto.Message { return &disk_protos.MigrateDiskTaskState{} },
	"filesystem.CreateFilesystem":                   func() proto.Message { return &filesystem_protos.CreateFilesystemTaskState{} },
	"filesystem.CreateExternalFilesystem":           func() proto.Message { return &filesystem_protos.CreateFilesystemTaskState{} },
	"filesystem.DeleteFilesystem":                   func() proto.Message { return &filesystem_protos.DeleteFilesystemTaskState{} },
	"filesystem.DeleteExternalFilesystem":           func() proto.Message { return &filesystem_protos.DeleteFilesystemTaskState{} },
	"filesystem.ResizeFilesystem":                   func() proto.Message { return &filesystem_protos.ResizeFilesystemTaskState{} },
	"images.CreateImageFromURL":                     func() proto.Message { return &images_protos.CreateImageFromURLTaskState{} },
	"images.CreateImageFromImage":                   func() proto.Message { return &images_protos.CreateImageFromImageTaskState{} },
	"images.CreateImageFromSnapshot":                func() proto.Message { return &images_protos.CreateImageFromSnapshotTaskState{} },
	"images.CreateImageFromDisk":                    func() proto.Message { return &images_protos.CreateImageFromDiskTaskState{} },
	"images.DeleteImage":                            func() proto.Message { return &images_protos.DeleteImageTaskState{} },
	"placement_group.CreatePlacementGroup":          func() proto.Message { return &placementgroup_protos.CreatePlacementGroupTaskState{} },
	"placement_group.DeletePlacementGroup":          func() proto.Message { return &placementgroup_protos.DeletePlacementGroupTaskState{} },
	"placement_group.AlterPlacementGroupMembership": func() proto.Message { return &placementgroup_protos.AlterPlacementGroupMembershipTaskState{} },
	"pools.AcquireBaseDisk":                         func() proto.Message { return &pools_protos.AcquireBaseDiskTaskState{} },
	"pools.CreateBaseDisk":                          func() proto.Message { return &pools_protos.CreateBaseDiskTaskState{} },
	"pools.ReleaseBaseDisk":                         func() proto.Message { return &pools_protos.ReleaseBaseDiskTaskState{} },
	"pools.RebaseOverlayDisk":                       func() proto.Message { return &pools_protos.RebaseOverlayDiskTaskState{} },
	"pools.ConfigurePool":                           func() proto.Message { return &pools_protos.ConfigurePoolTaskState{} },
	"pools.DeletePool":                              func() proto.Message { return &pools_protos.DeletePoolTaskState{} },
	"pools.ImageDeleting":                           func() proto.Message { return &pools_protos.ImageDeletingTaskState{} },
	"pools.OptimizeBaseDisks":                       func() proto.Message { return &pools_protos.OptimizeBaseDisksTaskState{} },
	"pools.RetireBaseDisk":                          func() proto.Message { return &pools_protos.RetireBaseDiskTaskState{} },
	"pools.RetireBaseDisks":                         func() proto.Message { return &pools_protos.RetireBaseDisksTaskState{} },
	"snapshots.CreateSnapshotFromDisk":              func() proto.Message { return &snapshot_protos.CreateSnapshotFromDiskTaskState{} },
	"snapshots.DeleteSnapshot":                      func() proto.Message { return &snapshot_protos.DeleteSnapshotTaskState{} },
}

type FormattableDuration struct {
	time.Duration
}

func (d FormattableDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

type TaskStateJSON struct {
	ID                        string               `json:"id"`
	IdempotencyKey            string               `json:"idempotency_key"`
	AccountID                 string               `json:"account_id"`
	TaskType                  string               `json:"task_type"`
	Regular                   bool                 `json:"regular"`
	Description               string               `json:"description"`
	StorageFolder             string               `json:"storage_folder"`
	CreatedAt                 time.Time            `json:"created_at"`
	CreatedBy                 string               `json:"created_by"`
	ModifiedAt                time.Time            `json:"modified_at"`
	GenerationID              uint64               `json:"generation_id"`
	Status                    string               `json:"status"`
	ErrorCode                 grpc_codes.Code      `json:"error_code"`
	ErrorMessage              string               `json:"error_message"`
	ErrorDetails              *errors.ErrorDetails `json:"error_details"`
	RetriableErrorCount       uint64               `json:"retriable_error_count"`
	Request                   proto.Message        `json:"request"`
	State                     proto.Message        `json:"state"`
	Metadata                  map[string]string    `json:"metadata"`
	Dependencies              []*TaskStateJSON     `json:"dependencies"`
	ChangedStateAt            time.Time            `json:"changed_state_at"`
	EndedAt                   time.Time            `json:"ended_at"`
	LastHost                  string               `json:"last_host"`
	LastRunner                string               `json:"last_runner"`
	ZoneID                    string               `json:"zone_id"`
	CloudID                   string               `json:"cloud_id"`
	FolderID                  string               `json:"folder_id"`
	InflightDuration          FormattableDuration  `json:"inflight_duration"`
	EstimatedInflightDuration FormattableDuration  `json:"estimated_inflight_duration"`
	StallingDuration          FormattableDuration  `json:"stalling_duration"`
	EstimatedStallingDuration FormattableDuration  `json:"estimated_stalling_duration"`
	WaitingDuration           FormattableDuration  `json:"waiting_duration"`
	PanicCount                uint64               `json:"panic_count"`
}

func (s *TaskStateJSON) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

////////////////////////////////////////////////////////////////////////////////

func TaskStateToJSON(state *storage.TaskState) *TaskStateJSON {
	var requestProto proto.Message

	creator, ok := requestProtoByTaskType[state.TaskType]
	if ok {
		requestProto = creator()
		err := proto.Unmarshal(state.Request, requestProto)
		if err != nil {
			requestProto = &empty.Empty{}
		}
	} else {
		requestProto = &empty.Empty{}
	}

	var stateProto proto.Message

	creator, ok = stateProtoByTaskType[state.TaskType]
	if ok {
		stateProto = creator()
		err := proto.Unmarshal(state.State, stateProto)
		if err != nil {
			stateProto = &empty.Empty{}
		}
	} else {
		stateProto = &empty.Empty{}
	}

	dependencies := make([]*TaskStateJSON, 0)
	for _, dep := range state.Dependencies.List() {
		dependencies = append(dependencies, &TaskStateJSON{ID: dep})
	}

	return &TaskStateJSON{
		ID:                        state.ID,
		IdempotencyKey:            state.IdempotencyKey,
		AccountID:                 state.AccountID,
		TaskType:                  state.TaskType,
		Regular:                   state.Regular,
		Description:               state.Description,
		StorageFolder:             state.StorageFolder,
		CreatedAt:                 state.CreatedAt,
		CreatedBy:                 state.CreatedBy,
		ModifiedAt:                state.ModifiedAt,
		GenerationID:              state.GenerationID,
		Status:                    storage.TaskStatusToString(state.Status),
		ErrorCode:                 state.ErrorCode,
		ErrorMessage:              state.ErrorMessage,
		ErrorDetails:              state.ErrorDetails,
		RetriableErrorCount:       state.RetriableErrorCount,
		Request:                   requestProto,
		State:                     stateProto,
		Metadata:                  state.Metadata.Vals(),
		Dependencies:              dependencies,
		ChangedStateAt:            state.ChangedStateAt,
		EndedAt:                   state.EndedAt,
		LastHost:                  state.LastHost,
		LastRunner:                state.LastRunner,
		ZoneID:                    state.ZoneID,
		InflightDuration:          FormattableDuration{state.InflightDuration},
		EstimatedInflightDuration: FormattableDuration{state.EstimatedInflightDuration},
		StallingDuration:          FormattableDuration{state.StallingDuration},
		EstimatedStallingDuration: FormattableDuration{state.EstimatedStallingDuration},
		WaitingDuration:           FormattableDuration{state.WaitingDuration},
		PanicCount:                state.PanicCount,
	}
}
