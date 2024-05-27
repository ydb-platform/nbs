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
	tx *persistence.Transaction,
) ([]pool, error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
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

	return pools, nil
}

func (s *storageYDB) getPoolBaseDisks(
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

		logging.Debug(
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

func (s *storageYDB) checkPoolConsistency(
	ctx context.Context,
	expectedPoolState pool,
	baseDisks []baseDisk,
) error {

	var actualPoolState pool
	for _, baseDisk := range baseDisks {
		logging.Debug(
			ctx,
			"processing baseDisk %+v for pool %+v",
			baseDisk,
			expectedPoolState,
		)

		if baseDisk.fromPool {
			actualPoolState.acquiredUnits += baseDisk.activeUnits

			if baseDisk.isInflight() {
				actualPoolState.baseDisksInflight += 1
			}
		}
	}

	if expectedPoolState.acquiredUnits != actualPoolState.acquiredUnits {
		return errors.NewNonRetriableErrorf(
			"actual acquiredUnits %v is not equal to expected %v for pool %+v",
			actualPoolState.acquiredUnits,
			expectedPoolState.acquiredUnits,
			expectedPoolState,
		)
	}

	if expectedPoolState.baseDisksInflight != actualPoolState.baseDisksInflight {
		return errors.NewNonRetriableErrorf(
			"actual baseDisksInflight %v is not equal to expected %v for pool %+v",
			actualPoolState.baseDisksInflight,
			expectedPoolState.baseDisksInflight,
			expectedPoolState,
		)
	}

	return nil
}

func (s *storageYDB) checkPoolsConsistency(
	ctx context.Context,
	session *persistence.Session,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	pools, err := s.getPools(ctx, tx)
	if err != nil {
		return err
	}

	for _, pool := range pools {
		baseDisks, err := s.getPoolBaseDisks(ctx, tx, pool)
		if err != nil {
			return err
		}

		err = s.checkPoolConsistency(ctx, pool, baseDisks)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) checkConsistency(
	ctx context.Context,
	session *persistence.Session,
) error {

	err := s.checkPoolsConsistency(ctx, session)
	if err != nil {
		return err
	}

	err = s.checkBaseDisksConsistency(ctx, session)
	if err != nil {
		return err
	}

	return nil
}
