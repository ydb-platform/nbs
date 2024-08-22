package storage

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) getPools(
	ctx context.Context,
	session *persistence.Session,
) ([]pool, error) {

	res, err := session.StreamExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";

		select *
		from pools
	`, s.tablesPath))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var pools []pool
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			pool, err := scanPool(res)
			if err != nil {
				return nil, err
			}

			pools = append(pools, pool)
		}
	}

	// NOTE: always check stream query result after iteration.
	err = res.Err()
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	return pools, nil
}

func (s *storageYDB) getBaseDisksFromPool(
	ctx context.Context,
	tx *persistence.Transaction,
	pool pool,
) ([]baseDisk, error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $zone_id as Utf8;
		declare $status as Int64;

		select *
		from base_disks
		where image_id = $image_id and zone_id = $zone_id
	`, s.tablesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(pool.imageID)),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(pool.zoneID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	baseDisks, err := scanBaseDisks(ctx, res)
	if err != nil {
		return nil, err
	}

	return baseDisks, nil
}

func (s *storageYDB) getBaseDisks(
	ctx context.Context,
	tx *persistence.Transaction,
) ([]baseDisk, error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";

		select *
		from base_disks
	`, s.tablesPath))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	baseDisks, err := scanBaseDisks(ctx, res)
	if err != nil {
		return nil, err
	}

	return baseDisks, nil
}

func (s *storageYDB) getSlots(
	ctx context.Context,
	tx *persistence.Transaction,
) ([]slot, error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";

		select *
		from slots
	`, s.tablesPath))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var slots []slot
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			slot, err := scanSlot(res)
			if err != nil {
				return nil, err
			}

			slots = append(slots, slot)
		}
	}

	return slots, nil
}

func (s *storageYDB) checkBaseDiskConsistency(
	ctx context.Context,
	baseDisk baseDisk,
	slots []slot,
) error {

	slotsBaseDiskCount := uint64(0)
	slotsTargetBaseDiskCount := uint64(0)
	for _, slot := range slots {
		if slot.status >= slotStatusReleased {
			continue
		}

		logging.Info(
			ctx,
			"processing slot %+v for baseDisk %+v",
			slot,
			baseDisk,
		)

		if baseDisk.id == slot.baseDiskID {
			slotsBaseDiskCount += 1
		}
		if baseDisk.id == slot.targetBaseDiskID {
			slotsTargetBaseDiskCount += 1
		}
	}

	if baseDisk.activeSlots != slotsBaseDiskCount+slotsTargetBaseDiskCount {
		return errors.NewNonRetriableErrorf(
			"base_disk %+v is in inconsistent state",
			baseDisk,
		)
	}

	return nil
}

func (s *storageYDB) checkBaseDisksConsistency(
	ctx context.Context,
	session *persistence.Session,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	baseDisks, err := s.getBaseDisks(ctx, tx)
	if err != nil {
		return err
	}

	slots, err := s.getSlots(ctx, tx)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	for _, baseDisk := range baseDisks {
		err = s.checkBaseDiskConsistency(ctx, baseDisk, slots)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *storageYDB) checkBaseDiskSlotReleased(
	ctx context.Context,
	session *persistence.Session,
	overlayDiskID string,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	slot, err := s.getAcquiredSlot(ctx, tx, overlayDiskID)
	if err != nil {
		return err
	}
	if slot == nil {
		return tx.Commit(ctx)
	}

	if slot.status != slotStatusReleased {
		return errors.NewNonRetriableErrorf(
			"internal inconsistency: slot %+v should be released",
			slot,
		)
	}
	return tx.Commit(ctx)
}

func (s *storageYDB) checkPoolConsistency(
	ctx context.Context,
	expectedPoolState pool,
	baseDisks []baseDisk,
) *PoolConsistencyCorrection {

	var actualPoolState pool
	for _, baseDisk := range baseDisks {
		logging.Debug(
			ctx,
			"checking consistency of baseDisk %+v from pool %+v",
			baseDisk,
			expectedPoolState,
		)

		if baseDisk.fromPool && baseDisk.status != baseDiskStatusCreationFailed {
			actualPoolState.size += baseDisk.freeSlots()
			actualPoolState.freeUnits += baseDisk.freeUnits()
			actualPoolState.acquiredUnits += baseDisk.activeUnits
		}

		if baseDisk.isInflight() {
			actualPoolState.baseDisksInflight += 1
		}
	}

	if expectedPoolState.size != actualPoolState.size ||
		expectedPoolState.freeUnits != actualPoolState.freeUnits ||
		expectedPoolState.acquiredUnits != actualPoolState.acquiredUnits ||
		expectedPoolState.baseDisksInflight != actualPoolState.baseDisksInflight {

		calculateDiff := func(a uint64, b uint64) int64 {
			return int64(a) - int64(b)
		}

		return &PoolConsistencyCorrection{
			ImageID:               expectedPoolState.imageID,
			ZoneID:                expectedPoolState.zoneID,
			SizeDiff:              calculateDiff(actualPoolState.size, expectedPoolState.size),
			FreeUnitsDiff:         calculateDiff(actualPoolState.freeUnits, expectedPoolState.freeUnits),
			AcquiredUnitsDiff:     calculateDiff(actualPoolState.acquiredUnits, expectedPoolState.acquiredUnits),
			BaseDisksInflightDiff: calculateDiff(actualPoolState.baseDisksInflight, expectedPoolState.baseDisksInflight),
		}
	}

	return nil
}

func (s *storageYDB) checkPoolsConsistency(
	ctx context.Context,
	session *persistence.Session,
) ([]PoolConsistencyCorrection, error) {

	var corrections []PoolConsistencyCorrection

	pools, err := s.getPools(ctx, session)
	if err != nil {
		return []PoolConsistencyCorrection{}, err
	}

	for _, pool := range pools {
		tx, err := session.BeginRWTransaction(ctx)
		if err != nil {
			return []PoolConsistencyCorrection{}, err
		}
		defer tx.Rollback(ctx)

		pool, err := s.getPoolOrDefault(ctx, tx, pool.imageID, pool.zoneID)
		if err != nil {
			return []PoolConsistencyCorrection{}, err
		}

		baseDisks, err := s.getBaseDisksFromPool(ctx, tx, pool)
		if err != nil {
			return []PoolConsistencyCorrection{}, err
		}

		err = tx.Commit(ctx)
		if err != nil {
			return []PoolConsistencyCorrection{}, err
		}

		correction := s.checkPoolConsistency(
			ctx,
			pool,
			baseDisks,
		)

		if correction != nil {
			corrections = append(corrections, *correction)
		}
	}

	return corrections, nil
}

func (s *storageYDB) checkConsistency(
	ctx context.Context,
	session *persistence.Session,
) error {

	corrections, err := s.checkPoolsConsistency(ctx, session)
	if err != nil {
		return err
	}

	if len(corrections) != 0 {
		return errors.NewNonRetriableErrorf(
			"internal inconsistency: pool consistency corrections are %+v",
			corrections,
		)
	}

	return s.checkBaseDisksConsistency(ctx, session)
}
