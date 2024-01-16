package storage

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

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

func (s *storageYDB) checkConsistency(
	ctx context.Context,
	session *persistence.Session,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";

		select *
		from base_disks
	`, s.tablesPath))
	if err != nil {
		return err
	}
	defer res.Close()

	baseDisks, err := scanBaseDisks(ctx, res)
	if err != nil {
		return err
	}

	res, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";

		select *
		from slots
	`, s.tablesPath))
	if err != nil {
		return err
	}
	defer res.Close()

	var slots []slot
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			slot, err := scanSlot(res)
			if err != nil {
				return err
			}

			slots = append(slots, slot)
		}
	}

	err = tx.Commit(ctx)
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
