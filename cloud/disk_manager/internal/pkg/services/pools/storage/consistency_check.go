package storage

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) checkPoolsConsistency(
	pools []pool,
	baseDisks []baseDisk,
	slots []slot,
) error {

	poolsActiveUnits := map[string]uint64{}
	baseDisksActiveUnits := map[string]uint64{}
	slotsActiveUnits := map[string]uint64{}

	for _, pool := range pools {
		poolsActiveUnits[pool.imageID] += pool.acquiredUnits
	}

	for _, baseDisk := range baseDisks {
		baseDisksActiveUnits[baseDisk.imageID] += baseDisk.activeUnits
	}

	for _, slot := range slots {
		slotsActiveUnits[slot.imageID] += slot.allottedUnits
	}

	for _, pool := range pools {
		if poolsActiveUnits[pool.imageID] != baseDisksActiveUnits[pool.imageID] {
			return errors.NewNonRetriableErrorf(
				"pool acquiredUnits %v is not equal to baseDisksActiveUnits %v",
				poolsActiveUnits[pool.imageID],
				baseDisksActiveUnits[pool.imageID],
			)
		}

		if poolsActiveUnits[pool.imageID] != slotsActiveUnits[pool.imageID] {
			return errors.NewNonRetriableErrorf(
				"pool acquiredUnits %v is not equal to slotsActiveUnits %v",
				pool.acquiredUnits,
				slotsActiveUnits[pool.imageID],
			)
		}
	}

	return nil
}

func (s *storageYDB) checkBaseDiskConsistency(
	baseDisk baseDisk,
	slots []slot,
) error {

	slotsBaseDiskCount := uint64(0)
	slotsTargetBaseDiskCount := uint64(0)
	for _, slot := range slots {
		if slot.status >= slotStatusReleased {
			continue
		}

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

func (s *storageYDB) checkConsistency(
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

	err = s.checkPoolsConsistency(pools, baseDisks, slots)
	if err != nil {
		return err
	}

	for _, baseDisk := range baseDisks {
		err = s.checkBaseDiskConsistency(baseDisk, slots)
		if err != nil {
			return err
		}
	}

	return nil
}
