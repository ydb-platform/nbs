package disks

import (
	"context"
	"math"
	"strings"

	"github.com/golang/protobuf/proto"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	disks_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/shards"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

func prepareDiskKind(kind disk_manager.DiskKind) (types.DiskKind, error) {
	switch kind {
	case disk_manager.DiskKind_DISK_KIND_UNSPECIFIED:
		return 0, errors.NewInvalidArgumentError("disk kind is required")
	case disk_manager.DiskKind_DISK_KIND_SSD:
		return types.DiskKind_DISK_KIND_SSD, nil
	case disk_manager.DiskKind_DISK_KIND_HDD:
		return types.DiskKind_DISK_KIND_HDD, nil
	case disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED:
		return types.DiskKind_DISK_KIND_SSD_NONREPLICATED, nil
	case disk_manager.DiskKind_DISK_KIND_SSD_MIRROR2:
		return types.DiskKind_DISK_KIND_SSD_MIRROR2, nil
	case disk_manager.DiskKind_DISK_KIND_SSD_MIRROR3:
		return types.DiskKind_DISK_KIND_SSD_MIRROR3, nil
	case disk_manager.DiskKind_DISK_KIND_SSD_LOCAL:
		return types.DiskKind_DISK_KIND_SSD_LOCAL, nil
	case disk_manager.DiskKind_DISK_KIND_HDD_NONREPLICATED:
		return types.DiskKind_DISK_KIND_HDD_NONREPLICATED, nil
	case disk_manager.DiskKind_DISK_KIND_HDD_LOCAL:
		return types.DiskKind_DISK_KIND_HDD_LOCAL, nil
	default:
		return 0, errors.NewInvalidArgumentError(
			"unknown disk kind %v",
			kind,
		)
	}
}

func prepareEncryptionMode(
	mode disk_manager.EncryptionMode,
) (types.EncryptionMode, error) {
	switch mode {
	case disk_manager.EncryptionMode_NO_ENCRYPTION:
		return types.EncryptionMode_NO_ENCRYPTION, nil
	case disk_manager.EncryptionMode_ENCRYPTION_AES_XTS:
		return types.EncryptionMode_ENCRYPTION_AES_XTS, nil
	case disk_manager.EncryptionMode_ENCRYPTION_AT_REST:
		return types.EncryptionMode_ENCRYPTION_AT_REST, nil
	default:
		return 0, errors.NewInvalidArgumentError(
			"unknown encryption mode %v",
			mode,
		)
	}
}

func PrepareEncryptionDesc(
	encryptionDesc *disk_manager.EncryptionDesc,
) (*types.EncryptionDesc, error) {

	if encryptionDesc == nil {
		return nil, nil
	}

	encryptionMode, err := prepareEncryptionMode(encryptionDesc.Mode)
	if err != nil {
		return nil, err
	}

	result := &types.EncryptionDesc{
		Mode: encryptionMode,
	}

	switch key := encryptionDesc.Key.(type) {
	case *disk_manager.EncryptionDesc_KeyHash:
		result.Key = &types.EncryptionDesc_KeyHash{
			KeyHash: key.KeyHash,
		}
	case *disk_manager.EncryptionDesc_KmsKey:
		result.Key = &types.EncryptionDesc_KmsKey{
			KmsKey: &types.KmsKey{
				KekId:        key.KmsKey.KekId,
				EncryptedDEK: key.KmsKey.EncryptedDek,
				TaskId:       key.KmsKey.TaskId,
			},
		}
	case nil:
		result.Key = nil
	default:
		return nil, errors.NewInvalidArgumentError("unknown key %s", key)
	}

	return result, nil
}

func getBlocksCountForSize(size uint64, blockSize uint32) (uint64, error) {
	if blockSize == 0 {
		return 0, errors.NewInvalidArgumentError(
			"invalid block size %v",
			blockSize,
		)
	}

	if size%uint64(blockSize) != 0 {
		return 0, errors.NewInvalidArgumentError(
			"invalid size %v for block size %v",
			size,
			blockSize,
		)
	}

	return size / uint64(blockSize), nil
}

////////////////////////////////////////////////////////////////////////////////

type service struct {
	taskScheduler   tasks.Scheduler
	taskStorage     tasks_storage.Storage
	config          *disks_config.DisksConfig
	nbsFactory      nbs.Factory
	poolService     pools.Service
	shardsService   shards.Service
	resourceStorage resources.Storage
}

func (s *service) prepareZoneId(
	ctx context.Context,
	req *disk_manager.CreateDiskRequest,
) (string, error) {

	diskMeta, err := s.resourceStorage.GetDiskMeta(ctx, req.DiskId.DiskId)
	if err != nil {
		return "", err
	}

	if diskMeta != nil {
		return diskMeta.ZoneID, nil
	}

	return s.shardsService.PickShard(ctx, req.DiskId, req.FolderId), nil
}

func (s *service) prepareCreateDiskParams(
	ctx context.Context,
	req *disk_manager.CreateDiskRequest,
) (*protos.CreateDiskParams, error) {

	if len(req.DiskId.ZoneId) == 0 ||
		len(req.DiskId.DiskId) == 0 {

		return nil, errors.NewInvalidArgumentError(
			"invalid disk id: %v",
			req.DiskId,
		)
	}

	zoneID, err := s.prepareZoneId(ctx, req)
	if err != nil {
		return nil, err
	}

	diskIDPrefix := s.config.GetCreationAndDeletionAllowedOnlyForDisksWithIdPrefix()
	if len(diskIDPrefix) != 0 && !strings.HasPrefix(req.DiskId.DiskId, diskIDPrefix) {
		return nil, errors.NewInvalidArgumentError(
			"can't create disk with id %q, because only disks with id prefix %q are allowed to be created",
			req.DiskId.DiskId,
			diskIDPrefix,
		)
	}

	if req.Size < 0 {
		return nil, errors.NewInvalidArgumentError(
			"invalid size: %v",
			req.Size,
		)
	}

	if req.BlockSize < 0 || req.BlockSize > math.MaxUint32 {
		return nil, errors.NewInvalidArgumentError(
			"invalid block size: %v",
			req.BlockSize,
		)
	}

	blockSize := uint32(req.BlockSize)
	if blockSize == 0 {
		blockSize = s.config.GetDefaultBlockSize()
	}

	blocksCount, err := getBlocksCountForSize(uint64(req.Size), blockSize)
	if err != nil {
		return nil, err
	}

	kind, err := prepareDiskKind(req.Kind)
	if err != nil {
		return nil, err
	}

	encryptionDesc, err := PrepareEncryptionDesc(req.EncryptionDesc)
	if err != nil {
		return nil, err
	}

	if req.TabletVersion < 0 || req.TabletVersion > math.MaxUint32 {
		return nil, errors.NewInvalidArgumentError(
			"invalid tablet version: %v",
			req.TabletVersion,
		)
	}

	tabletVersion := uint32(req.TabletVersion)

	if nbs.IsDiskRegistryBasedDisk(kind) && s.config.GetDisableDiskRegistryBasedDisks() {
		allowed := false
		for _, folderID := range s.config.GetDiskRegistryBasedDisksFolderIdAllowList() {
			if folderID == req.FolderId {
				allowed = true
				break
			}
		}

		if !allowed {
			return nil, errors.NewInvalidArgumentError(
				"can't create a DiskRegistry based disk with id %q, because "+
					"it is not allowed for the %q folder",
				req.DiskId.DiskId,
				req.FolderId,
			)
		}
	}

	return &protos.CreateDiskParams{
		BlocksCount: blocksCount,
		Disk: &types.Disk{
			ZoneId: zoneID,
			DiskId: req.DiskId.DiskId,
		},
		BlockSize:               blockSize,
		Kind:                    kind,
		CloudId:                 req.CloudId,
		FolderId:                req.FolderId,
		TabletVersion:           tabletVersion,
		PlacementGroupId:        req.PlacementGroupId,
		PlacementPartitionIndex: req.PlacementPartitionIndex,
		StoragePoolName:         req.StoragePoolName,
		AgentIds:                req.AgentIds,
		EncryptionDesc:          encryptionDesc,
	}, nil
}

func (s *service) areOverlayDisksSupportedForDiskKind(kind types.DiskKind) bool {
	return kind == types.DiskKind_DISK_KIND_SSD ||
		kind == types.DiskKind_DISK_KIND_HDD ||
		(s.config.GetEnableOverlayDiskRegistryBasedDisks() &&
			nbs.IsDiskRegistryBasedDisk(kind))
}

func (s *service) isOverlayDiskAllowed(
	ctx context.Context,
	req *disk_manager.CreateDiskRequest,
	srcImageID string,
	params *protos.CreateDiskParams,
) (bool, error) {

	compatibleWithBaseDisksFromPools :=
		s.areOverlayDisksSupportedForDiskKind(params.Kind) &&
			params.BlockSize == 4096 &&
			req.Size <= 4<<40 // 4 TB - maximum base disk size
	if !compatibleWithBaseDisksFromPools {
		return false, nil
	}

	if req.ForceNotLayered {
		return false, nil
	}

	if req.EncryptionDesc != nil &&
		req.EncryptionDesc.Mode != disk_manager.EncryptionMode_NO_ENCRYPTION {
		// Encrypted overlay disks are not yet supported.
		return false, nil
	}

	imageMeta, err := s.resourceStorage.GetImageMeta(ctx, srcImageID)
	if err != nil {
		return false, err
	}

	if imageMeta != nil &&
		imageMeta.Encryption != nil &&
		imageMeta.Encryption.Mode != types.EncryptionMode_NO_ENCRYPTION {
		// Overlay disks from encrypted images are not yet supported.
		return false, nil
	}

	configured, err := s.poolService.IsPoolConfigured(
		ctx,
		srcImageID,
		params.Disk.ZoneId,
	)
	if err != nil {
		return false, err
	}

	if !configured {
		return false, nil
	}

	if common.Find(s.config.GetOverlayDisksFolderIdWhitelist(), params.FolderId) {
		return true, nil
	}

	if common.Find(s.config.GetOverlayDisksFolderIdBlacklist(), params.FolderId) {
		return false, nil
	}

	return !s.config.GetDisableOverlayDisks(), nil
}

func (s *service) CreateDisk(
	ctx context.Context,
	req *disk_manager.CreateDiskRequest,
) (string, error) {

	params, err := s.prepareCreateDiskParams(ctx, req)
	if err != nil {
		return "", err
	}

	switch src := req.Src.(type) {
	case *disk_manager.CreateDiskRequest_SrcEmpty:
		return s.taskScheduler.ScheduleTask(
			ctx,
			"disks.CreateEmptyDisk",
			"",
			params,
		)
	case *disk_manager.CreateDiskRequest_SrcImageId:
		allowed, err := s.isOverlayDiskAllowed(ctx, req, src.SrcImageId, params)
		if err != nil {
			return "", err
		}

		if allowed {
			return s.taskScheduler.ScheduleTask(
				ctx,
				"disks.CreateOverlayDisk",
				"",
				&protos.CreateOverlayDiskRequest{
					SrcImageId: src.SrcImageId,
					Params:     params,
				},
			)
		}

		return s.taskScheduler.ScheduleTask(
			ctx,
			"disks.CreateDiskFromImage",
			"",
			&protos.CreateDiskFromImageRequest{
				SrcImageId: src.SrcImageId,
				Params:     params,
			},
		)
	case *disk_manager.CreateDiskRequest_SrcSnapshotId:
		return s.taskScheduler.ScheduleTask(
			ctx,
			"disks.CreateDiskFromSnapshot",
			"",
			&protos.CreateDiskFromSnapshotRequest{
				SrcSnapshotId: src.SrcSnapshotId,
				Params:        params,
			},
		)
	default:
		return "", errors.NewInvalidArgumentError("unknown src %s", src)
	}
}

func (s *service) DeleteDisk(
	ctx context.Context,
	req *disk_manager.DeleteDiskRequest,
) (string, error) {

	if len(req.DiskId.DiskId) == 0 {
		return "", errors.NewInvalidArgumentError(
			"disk id is empty, req=%v",
			req,
		)
	}

	diskIDPrefix := s.config.GetCreationAndDeletionAllowedOnlyForDisksWithIdPrefix()
	if len(diskIDPrefix) != 0 && !strings.HasPrefix(req.DiskId.DiskId, diskIDPrefix) {
		return "", errors.NewInvalidArgumentError(
			"can't delete disk with id %q, because only disks with id prefix %q are allowed to be deleted",
			req.DiskId.DiskId,
			diskIDPrefix,
		)
	}

	return s.taskScheduler.ScheduleTask(
		ctx,
		"disks.DeleteDisk",
		"",
		&protos.DeleteDiskRequest{
			Disk: &types.Disk{
				ZoneId: req.DiskId.ZoneId,
				DiskId: req.DiskId.DiskId,
			},
			Sync: req.Sync,
		},
	)
}

func (s *service) ResizeDisk(
	ctx context.Context,
	req *disk_manager.ResizeDiskRequest,
) (string, error) {

	if len(req.DiskId.ZoneId) == 0 || len(req.DiskId.DiskId) == 0 {
		return "", errors.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	if req.Size < 0 {
		return "", errors.NewInvalidArgumentError(
			"invalid size: %v",
			req.Size,
		)
	}

	return s.taskScheduler.ScheduleTask(
		ctx,
		"disks.ResizeDisk",
		"",
		&protos.ResizeDiskRequest{
			Disk: &types.Disk{
				ZoneId: req.DiskId.ZoneId,
				DiskId: req.DiskId.DiskId,
			},
			Size: uint64(req.Size),
		},
	)
}

func (s *service) AlterDisk(
	ctx context.Context,
	req *disk_manager.AlterDiskRequest,
) (string, error) {

	if len(req.DiskId.ZoneId) == 0 || len(req.DiskId.DiskId) == 0 {
		return "", errors.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(
		ctx,
		"disks.AlterDisk",
		"",
		&protos.AlterDiskRequest{
			Disk: &types.Disk{
				ZoneId: req.DiskId.ZoneId,
				DiskId: req.DiskId.DiskId,
			},
			CloudId:  req.CloudId,
			FolderId: req.FolderId,
		},
	)
}

func (s *service) AssignDisk(
	ctx context.Context,
	req *disk_manager.AssignDiskRequest,
) (string, error) {

	if len(req.DiskId.ZoneId) == 0 || len(req.DiskId.DiskId) == 0 {
		return "", errors.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(
		ctx,
		"disks.AssignDisk",
		"",
		&protos.AssignDiskRequest{
			Disk: &types.Disk{
				ZoneId: req.DiskId.ZoneId,
				DiskId: req.DiskId.DiskId,
			},
			InstanceId: req.InstanceId,
			Host:       req.Host,
			Token:      req.Token,
		},
	)
}

func (s *service) UnassignDisk(
	ctx context.Context,
	req *disk_manager.UnassignDiskRequest,
) (string, error) {

	if len(req.DiskId.ZoneId) == 0 || len(req.DiskId.DiskId) == 0 {
		return "", errors.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(
		ctx,
		"disks.UnassignDisk",
		"",
		&protos.UnassignDiskRequest{
			Disk: &types.Disk{
				ZoneId: req.DiskId.ZoneId,
				DiskId: req.DiskId.DiskId,
			},
		},
	)
}

func (s *service) DescribeDiskModel(
	ctx context.Context,
	req *disk_manager.DescribeDiskModelRequest,
) (*disk_manager.DiskModel, error) {

	var client nbs.Client
	var err error

	if len(req.ZoneId) == 0 {
		client, err = s.nbsFactory.GetClientFromDefaultZone(ctx)
	} else {
		client, err = s.nbsFactory.GetClient(ctx, req.ZoneId)
	}
	if err != nil {
		return nil, err
	}

	if req.Size < 0 {
		return nil, errors.NewInvalidArgumentError(
			"invalid size: %v",
			req.Size,
		)
	}

	if req.BlockSize < 0 || req.BlockSize > math.MaxUint32 {
		return nil, errors.NewInvalidArgumentError(
			"invalid block size: %v",
			req.BlockSize,
		)
	}

	blockSize := uint32(req.BlockSize)
	if blockSize == 0 {
		blockSize = s.config.GetDefaultBlockSize()
	}

	if req.TabletVersion < 0 || req.TabletVersion > math.MaxUint32 {
		return nil, errors.NewInvalidArgumentError(
			"invalid tablet version: %v",
			req.TabletVersion,
		)
	}

	kind, err := prepareDiskKind(req.Kind)
	if err != nil {
		return nil, err
	}

	blocksCount, err := getBlocksCountForSize(uint64(req.Size), blockSize)
	if err != nil {
		return nil, err
	}

	model, err := client.DescribeModel(
		ctx,
		blocksCount,
		uint32(req.BlockSize),
		kind,
		uint32(req.TabletVersion),
	)
	if err != nil {
		return nil, err
	}

	return &disk_manager.DiskModel{
		BlockSize:     int64(model.BlockSize),
		Size:          int64(uint64(model.BlockSize) * model.BlocksCount),
		ChannelsCount: int64(model.ChannelsCount),
		Kind:          req.Kind,
		PerformanceProfile: &disk_manager.DiskPerformanceProfile{
			MaxReadBandwidth:   int64(model.PerformanceProfile.MaxReadBandwidth),
			MaxPostponedWeight: int64(model.PerformanceProfile.MaxPostponedWeight),
			ThrottlingEnabled:  model.PerformanceProfile.ThrottlingEnabled,
			MaxReadIops:        int64(model.PerformanceProfile.MaxReadIops),
			BoostTime:          int64(model.PerformanceProfile.BoostTime),
			BoostRefillTime:    int64(model.PerformanceProfile.BoostRefillTime),
			BoostPercentage:    int64(model.PerformanceProfile.BoostPercentage),
			MaxWriteBandwidth:  int64(model.PerformanceProfile.MaxWriteBandwidth),
			MaxWriteIops:       int64(model.PerformanceProfile.MaxWriteIops),
			BurstPercentage:    int64(model.PerformanceProfile.BurstPercentage),
		},
		MergedChannelsCount: int64(model.MergedChannelsCount),
		MixedChannelsCount:  int64(model.MixedChannelsCount),
	}, nil
}

func (s *service) StatDisk(
	ctx context.Context,
	req *disk_manager.StatDiskRequest,
) (*disk_manager.DiskStats, error) {

	if len(req.DiskId.ZoneId) == 0 ||
		len(req.DiskId.DiskId) == 0 {

		return nil, errors.NewInvalidArgumentError(
			"invalid disk id: %v",
			req.DiskId,
		)
	}

	client, err := s.nbsFactory.GetClient(ctx, req.DiskId.ZoneId)
	if err != nil {
		return nil, err
	}

	stats, err := client.Stat(ctx, req.DiskId.DiskId)
	if err != nil {
		return nil, err
	}

	return &disk_manager.DiskStats{StorageSize: int64(stats.StorageSize)}, nil
}

func (s *service) MigrateDisk(
	ctx context.Context,
	req *disk_manager.MigrateDiskRequest,
) (string, error) {

	if len(req.DiskId.ZoneId) == 0 || len(req.DiskId.DiskId) == 0 ||
		len(req.DstZoneId) == 0 {

		return "", errors.NewInvalidArgumentError(
			"some of parameters are empty, req=%v",
			req,
		)
	}
	if req.DiskId.ZoneId == req.DstZoneId {
		return "", errors.NewInvalidArgumentError(
			"cannot migrate disk to the same zone, req=%v",
			req,
		)
	}

	return s.taskScheduler.ScheduleTask(
		ctx,
		"disks.MigrateDisk",
		"",
		&protos.MigrateDiskRequest{
			Disk: &types.Disk{
				ZoneId: req.DiskId.ZoneId,
				DiskId: req.DiskId.DiskId,
			},
			DstZoneId:                  req.DstZoneId,
			DstPlacementGroupId:        req.DstPlacementGroupId,
			DstPlacementPartitionIndex: req.DstPlacementPartitionIndex,
		},
	)
}

func (s *service) SendMigrationSignal(
	ctx context.Context,
	req *disk_manager.SendMigrationSignalRequest,
) error {

	task, err := s.taskStorage.GetTask(ctx, req.OperationId)
	if err != nil {
		return err
	}

	state := &protos.MigrateDiskTaskState{}
	err = proto.Unmarshal(task.State, state)
	if err != nil {
		return err
	}

	switch req.Signal {
	case disk_manager.SendMigrationSignalRequest_FINISH_REPLICATION:
		if state.Status < protos.MigrationStatus_Replicating {
			return task_errors.NewNonRetriableErrorf(
				"Invalid migration status %v",
				state.Status,
			)
		}

		return s.taskScheduler.SendEvent(
			ctx,
			state.ReplicateTaskID,
			int64(dataplane_protos.ReplicateDiskTaskEvents_FINISH_REPLICATION),
		)
	case disk_manager.SendMigrationSignalRequest_FINISH_MIGRATION:
		return s.taskScheduler.SendEvent(
			ctx,
			task.ID,
			int64(protos.MigrateDiskTaskEvents_FINISH_MIGRATION),
		)
	}

	return errors.NewInvalidArgumentError(
		"migration signal is invalid, req=%v",
		req,
	)
}

func (s *service) DescribeDisk(
	ctx context.Context,
	req *disk_manager.DescribeDiskRequest,
) (*disk_manager.DiskParams, error) {

	if len(req.DiskId.ZoneId) == 0 ||
		len(req.DiskId.DiskId) == 0 {

		return nil, errors.NewInvalidArgumentError(
			"invalid disk id: %v",
			req.DiskId,
		)
	}

	client, err := s.nbsFactory.GetClient(ctx, req.DiskId.ZoneId)
	if err != nil {
		return nil, err
	}

	params, err := client.Describe(ctx, req.DiskId.DiskId)
	if err != nil {
		return nil, err
	}

	return &disk_manager.DiskParams{
		BlockSize: int64(params.BlockSize),
		Size:      int64(uint64(params.BlockSize) * params.BlocksCount),
		CloudId:   params.CloudID,
		FolderId:  params.FolderID,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func NewService(
	taskScheduler tasks.Scheduler,
	taskStorage tasks_storage.Storage,
	config *disks_config.DisksConfig,
	nbsFactory nbs.Factory,
	poolService pools.Service,
	shardsService shards.Service,
	resourceStorage resources.Storage,
) Service {

	return &service{
		taskScheduler:   taskScheduler,
		taskStorage:     taskStorage,
		config:          config,
		nbsFactory:      nbsFactory,
		poolService:     poolService,
		shardsService:   shardsService,
		resourceStorage: resourceStorage,
	}
}
