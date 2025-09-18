package disks

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type createEmptyDiskTask struct {
	storage      resources.Storage
	scheduler    tasks.Scheduler
	nbsFactory   nbs.Factory
	params       *protos.CreateDiskParams
	state        *protos.CreateEmptyDiskTaskState
	cellSelector cells.CellSelector
}

func (t *createEmptyDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createEmptyDiskTask) Load(request, state []byte) error {
	t.params = &protos.CreateDiskParams{}
	err := proto.Unmarshal(request, t.params)
	if err != nil {
		return err
	}

	t.state = &protos.CreateEmptyDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createEmptyDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	var client nbs.Client
	var err error

	if len(t.state.SelectedCellId) > 0 {
		client, err = t.nbsFactory.GetClient(ctx, t.state.SelectedCellId)
		if err != nil {
			return err
		}
	} else {
		client, err = t.cellSelector.SelectCell(
			ctx,
			t.params.Disk.ZoneId,
			t.params.FolderId,
		)
		if err != nil {
			return err
		}

		t.state.SelectedCellId = client.ZoneID()
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	selfTaskID := execCtx.GetTaskID()

	diskMeta, err := t.storage.CreateDisk(ctx, resources.DiskMeta{
		ID:          t.params.Disk.DiskId,
		ZoneID:      t.state.SelectedCellId,
		BlocksCount: t.params.BlocksCount,
		BlockSize:   t.params.BlockSize,
		Kind:        common.DiskKindToString(t.params.Kind),
		CloudID:     t.params.CloudId,
		FolderID:    t.params.FolderId,

		CreateRequest: t.params,
		CreateTaskID:  selfTaskID,
		CreatingAt:    time.Now(),
		CreatedBy:     "", // TODO: Extract CreatedBy from execCtx
	})
	if err != nil {
		return err
	}

	if diskMeta == nil {
		return errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			t.params.Disk.DiskId,
		)
	}

	err = client.Create(ctx, nbs.CreateDiskParams{
		ID:                      t.params.Disk.DiskId,
		BlocksCount:             t.params.BlocksCount,
		BlockSize:               t.params.BlockSize,
		Kind:                    t.params.Kind,
		CloudID:                 t.params.CloudId,
		FolderID:                t.params.FolderId,
		TabletVersion:           t.params.TabletVersion,
		PlacementGroupID:        t.params.PlacementGroupId,
		PlacementPartitionIndex: t.params.PlacementPartitionIndex,
		StoragePoolName:         t.params.StoragePoolName,
		AgentIds:                t.params.AgentIds,
		EncryptionDesc:          t.params.EncryptionDesc,
	})
	if err != nil {
		return err
	}

	diskMeta.CreatedAt = time.Now()
	return t.storage.DiskCreated(ctx, *diskMeta)
}

func (t *createEmptyDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()

	diskMeta, err := t.storage.DeleteDisk(
		ctx,
		t.params.Disk.DiskId,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if diskMeta == nil {
		return nil
	}

	client, err := t.nbsFactory.GetClient(ctx, diskMeta.ZoneID)
	if err != nil {
		return err
	}

	err = client.Delete(ctx, diskMeta.ID)
	if err != nil {
		return err
	}

	return t.storage.DiskDeleted(ctx, diskMeta.ID, time.Now())
}

func (t *createEmptyDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &disk_manager.CreateDiskMetadata{}, nil
}

func (t *createEmptyDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
