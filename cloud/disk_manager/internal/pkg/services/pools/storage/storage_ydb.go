package storage

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	common_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	pools_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type storageYDB struct {
	db                                 *persistence.YDBClient
	tablesPath                         string
	metrics                            metrics.Metrics
	maxActiveSlots                     uint64
	maxBaseDisksInflight               uint64
	maxBaseDiskUnits                   uint64
	takeBaseDisksToScheduleParallelism int
	baseDiskIDPrefix                   string
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) AcquireBaseDiskSlot(
	ctx context.Context,
	imageID string,
	slot Slot,
) (BaseDisk, error) {

	var baseDisk BaseDisk

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			baseDisk, err = s.acquireBaseDiskSlot(
				ctx,
				session,
				imageID,
				slot,
			)
			return err
		},
	)
	return baseDisk, err
}

func (s *storageYDB) ReleaseBaseDiskSlot(
	ctx context.Context,
	overlayDisk *types.Disk,
) (BaseDisk, error) {

	var baseDisk BaseDisk

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			baseDisk, err = s.releaseBaseDiskSlot(
				ctx,
				session,
				overlayDisk,
			)
			return err
		},
	)
	return baseDisk, err
}

func (s *storageYDB) RelocateOverlayDiskTx(
	ctx context.Context,
	tx *persistence.Transaction,
	overlayDisk *types.Disk,
	targetZoneID string,
) (RebaseInfo, error) {

	return s.relocateOverlayDiskTx(ctx, tx, overlayDisk, targetZoneID)
}

func (s *storageYDB) OverlayDiskRebasing(
	ctx context.Context,
	info RebaseInfo,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.overlayDiskRebasing(
				ctx,
				session,
				info,
			)
		},
	)
}

func (s *storageYDB) OverlayDiskRebased(
	ctx context.Context,
	info RebaseInfo,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.overlayDiskRebased(
				ctx,
				session,
				info,
			)
		},
	)
}

func (s *storageYDB) BaseDiskCreated(
	ctx context.Context,
	baseDisk BaseDisk,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.baseDiskCreated(ctx, session, baseDisk)
		},
	)
}

func (s *storageYDB) BaseDiskCreationFailed(
	ctx context.Context,
	baseDisk BaseDisk,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.baseDiskCreationFailed(ctx, session, baseDisk)
		},
	)
}

func (s *storageYDB) TakeBaseDisksToSchedule(
	ctx context.Context,
) ([]BaseDisk, error) {

	var baseDisks []BaseDisk

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			baseDisks, err = s.takeBaseDisksToSchedule(ctx, session)
			return err
		},
	)
	return baseDisks, err
}

func (s *storageYDB) GetReadyPoolInfos(
	ctx context.Context,
) ([]PoolInfo, error) {

	var poolInfos []PoolInfo

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			poolInfos, err = s.getReadyPoolInfos(ctx, session)
			return err
		},
	)
	return poolInfos, err
}

func (s *storageYDB) BaseDisksScheduled(
	ctx context.Context,
	baseDisks []BaseDisk,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.baseDisksScheduled(ctx, session, baseDisks)
		},
	)
}

func (s *storageYDB) ConfigurePool(
	ctx context.Context,
	imageID string,
	zoneID string,
	capacity uint32,
	imageSize uint64,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.configurePool(
				ctx,
				session,
				imageID,
				zoneID,
				capacity,
				imageSize,
			)
		},
	)
}

func (s *storageYDB) IsPoolConfigured(
	ctx context.Context,
	imageID string,
	zoneID string,
) (bool, error) {

	var res bool

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			res, err = s.isPoolConfigured(ctx, session, imageID, zoneID)
			return err
		},
	)
	return res, err
}

func (s *storageYDB) DeletePool(
	ctx context.Context,
	imageID string,
	zoneID string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.deletePool(ctx, session, imageID, zoneID)
		},
	)
}

func (s *storageYDB) ImageDeleting(
	ctx context.Context,
	imageID string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.imageDeleting(ctx, session, imageID)
		},
	)
}

func (s *storageYDB) GetBaseDisksToDelete(
	ctx context.Context,
	limit uint64,
) ([]BaseDisk, error) {

	var baseDisks []BaseDisk

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			baseDisks, err = s.getBaseDisksToDelete(ctx, session, limit)
			return err
		},
	)
	return baseDisks, err
}

func (s *storageYDB) BaseDisksDeleted(
	ctx context.Context,
	baseDisks []BaseDisk,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.baseDisksDeleted(ctx, session, baseDisks)
		},
	)
}
func (s *storageYDB) ClearDeletedBaseDisks(
	ctx context.Context,
	deletedBefore time.Time,
	limit int,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.clearDeletedBaseDisks(ctx, session, deletedBefore, limit)
		},
	)
}

func (s *storageYDB) ClearReleasedSlots(
	ctx context.Context,
	releasedBefore time.Time,
	limit int,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.clearReleasedSlots(ctx, session, releasedBefore, limit)
		},
	)
}

func (s *storageYDB) RetireBaseDisk(
	ctx context.Context,
	baseDiskID string,
	srcDisk *types.Disk,
	useImageSize uint64,
) ([]RebaseInfo, error) {

	var rebaseInfos []RebaseInfo

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			rebaseInfos, err = s.retireBaseDisk(
				ctx,
				session,
				baseDiskID,
				srcDisk,
				useImageSize,
			)
			return err
		},
	)
	return rebaseInfos, err
}

func (s *storageYDB) IsBaseDiskRetired(
	ctx context.Context,
	baseDiskID string,
) (bool, error) {

	var retired bool

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			retired, err = s.isBaseDiskRetired(ctx, session, baseDiskID)
			return err
		},
	)
	return retired, err
}

func (s *storageYDB) ListBaseDisks(
	ctx context.Context,
	imageID string,
	zoneID string,
) ([]BaseDisk, error) {

	var baseDisks []BaseDisk

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			baseDisks, err = s.listBaseDisks(ctx, session, imageID, zoneID)
			return err
		},
	)
	return baseDisks, err
}

func (s *storageYDB) LockPool(
	ctx context.Context,
	imageID string,
	zoneID string,
	lockID string,
) (bool, error) {

	var locked bool

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			locked, err = s.lockPool(ctx, session, imageID, zoneID, lockID)
			return err
		},
	)
	return locked, err
}

func (s *storageYDB) UnlockPool(
	ctx context.Context,
	imageID string,
	zoneID string,
	lockID string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.unlockPool(ctx, session, imageID, zoneID, lockID)
		},
	)
}

func (s *storageYDB) CheckPoolsConsistency(
	ctx context.Context,
) ([]PoolConsistencyCorrection, error) {

	var corrections []PoolConsistencyCorrection

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			corrections, err = s.checkPoolsConsistency(ctx, session)
			return err
		},
	)
	return corrections, err
}

func (s *storageYDB) CheckBaseDisksConsistency(ctx context.Context) error {
	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.checkBaseDisksConsistency(ctx, session)
		},
	)
}

func (s *storageYDB) CheckBaseDiskSlotReleased(
	ctx context.Context,
	overlayDiskID string,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.checkBaseDiskSlotReleased(ctx, session, overlayDiskID)
		},
	)
}

func (s *storageYDB) CheckConsistency(ctx context.Context) error {
	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.checkConsistency(ctx, session)
		},
	)
}

////////////////////////////////////////////////////////////////////////////////

func NewStorage(
	config *pools_config.PoolsConfig,
	db *persistence.YDBClient,
	metricsRegistry common_metrics.Registry,
) (Storage, error) {

	common.Assert(
		config.GetMaxActiveSlots() != 0,
		"MaxActiveSlots should not be zero",
	)
	common.Assert(
		config.GetMaxBaseDisksInflight() != 0,
		"GetMaxBaseDisksInflight should not be zero",
	)
	common.Assert(
		config.GetMaxBaseDiskUnits() != 0,
		"MaxBaseDiskUnits should not be zero",
	)

	// Default value.
	takeBaseDisksToScheduleParallelism := 1
	if config.GetTakeBaseDisksToScheduleParallelism() != 0 {
		takeBaseDisksToScheduleParallelism = int(config.GetTakeBaseDisksToScheduleParallelism())
	}

	metrics := metrics.New(metricsRegistry)

	return &storageYDB{
		db:                                 db,
		tablesPath:                         db.AbsolutePath(config.GetStorageFolder()),
		metrics:                            metrics,
		maxActiveSlots:                     uint64(config.GetMaxActiveSlots()),
		maxBaseDisksInflight:               uint64(config.GetMaxBaseDisksInflight()),
		maxBaseDiskUnits:                   uint64(config.GetMaxBaseDiskUnits()),
		takeBaseDisksToScheduleParallelism: takeBaseDisksToScheduleParallelism,
		baseDiskIDPrefix:                   config.GetBaseDiskIdPrefix(),
	}, nil
}
