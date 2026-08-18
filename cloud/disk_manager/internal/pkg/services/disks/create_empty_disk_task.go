package disks

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs2"
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
	nbs2Factory  nbs2.Factory
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

	if common.IsNbs2DiskKind(t.params.Kind) {
		return t.runNbs2(ctx, execCtx)
	}

	client, err := SelectCellForDisk(
		ctx,
		execCtx,
		t.state,
		t.params,
		t.cellSelector,
		t.nbsFactory,
		t.storage,
	)
	if err != nil {
		return err
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

func (t *createEmptyDiskTask) runNbs2(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.nbs2Factory == nil {
		return errors.NewNonCancellableErrorf(
			"nbs2 client is not configured",
		)
	}

	if len(t.state.SelectedCellId) == 0 {
		t.state.SelectedCellId = t.params.Disk.ZoneId
		err := execCtx.SaveState(ctx)
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
		TabletID:    t.state.TabletId,

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

	if len(t.state.TabletId) == 0 {
		client, err := t.nbs2Factory.GetClient(ctx, t.state.SelectedCellId)
		if err != nil {
			return err
		}

		tabletID, err := client.CreatePartition(ctx, nbs2.CreatePartitionParams{
			DiskID:          t.params.Disk.DiskId,
			BlockSize:       t.params.BlockSize,
			BlocksCount:     t.params.BlocksCount,
			StoragePoolName: t.params.StoragePoolName,
		})
		if err != nil {
			return err
		}

		t.state.TabletId = tabletID
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	diskMeta.TabletID = t.state.TabletId
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

	if common.IsNbs2DiskKind(t.params.Kind) ||
		common.IsNbs2DiskKindString(diskMeta.Kind) {

		tabletID := t.state.GetTabletId()
		if len(diskMeta.TabletID) > 0 {
			tabletID = diskMeta.TabletID
		}
		if len(tabletID) > 0 && t.nbs2Factory != nil {
			client, err := t.nbs2Factory.GetClient(ctx, diskMeta.ZoneID)
			if err != nil {
				return err
			}

			err = client.DeletePartition(ctx, tabletID)
			if err != nil {
				return err
			}
		}

		return t.storage.DiskDeleted(ctx, diskMeta.ID, time.Now())
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
