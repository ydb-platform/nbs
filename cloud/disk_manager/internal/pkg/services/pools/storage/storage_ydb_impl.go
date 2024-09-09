package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) findBaseDisks(
	ctx context.Context,
	session *persistence.Session,
	ids []string,
) ([]baseDisk, error) {

	if len(ids) == 0 {
		return nil, nil
	}

	var values []persistence.Value
	for _, id := range ids {
		values = append(values, persistence.UTF8Value(id))
	}

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $ids as List<Utf8>;

		select *
		from base_disks
		where id in $ids
	`, s.tablesPath),
		persistence.ValueParam("$ids", persistence.ListValue(values...)),
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

func (s *storageYDB) findBaseDisksTx(
	ctx context.Context,
	tx *persistence.Transaction,
	ids []string,
) ([]baseDisk, error) {

	if len(ids) == 0 {
		return nil, nil
	}

	var values []persistence.Value
	for _, id := range ids {
		values = append(values, persistence.UTF8Value(id))
	}

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $ids as List<Utf8>;

		select *
		from base_disks
		where id in $ids
	`, s.tablesPath),
		persistence.ValueParam("$ids", persistence.ListValue(values...)),
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

func (s *storageYDB) findBaseDisk(
	ctx context.Context,
	tx *persistence.Transaction,
	id string,
) (*baseDisk, error) {

	baseDisks, err := s.findBaseDisksTx(ctx, tx, []string{id})
	if err != nil {
		return nil, err
	}

	if len(baseDisks) == 0 {
		return nil, nil
	}

	return &baseDisks[0], nil
}

func (s *storageYDB) getBaseDisk(
	ctx context.Context,
	tx *persistence.Transaction,
	id string,
) (baseDisk, error) {

	out, err := s.findBaseDisk(ctx, tx, id)
	if err != nil {
		return baseDisk{}, err
	}

	if out == nil {
		err = tx.Commit(ctx)
		if err != nil {
			return baseDisk{}, err
		}

		return baseDisk{}, errors.NewNonRetriableErrorf(
			"failed to find base disk with id %v",
			id,
		)
	}

	return *out, nil
}

func (s *storageYDB) acquireBaseDiskSlotIdempotent(
	ctx context.Context,
	tx *persistence.Transaction,
	slot slot,
) (BaseDisk, error) {

	if slot.status == slotStatusReleased {
		err := tx.Commit(ctx)
		if err != nil {
			return BaseDisk{}, err
		}

		return BaseDisk{}, errors.NewSilentNonRetriableErrorf(
			"slot %+v is already released",
			slot,
		)
	}

	baseDisk, err := s.getBaseDisk(ctx, tx, slot.baseDiskID)
	if err != nil {
		return BaseDisk{}, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return BaseDisk{}, err
	}

	if baseDisk.isDoomed() {
		return BaseDisk{}, errors.NewNonRetriableErrorf(
			"base disk with id %v is already deleting/deleted, slot %+v",
			baseDisk.id,
			slot,
		)
	}

	return baseDisk.toBaseDisk(), nil
}

func (s *storageYDB) updateFreeTable(
	ctx context.Context,
	tx *persistence.Transaction,
	transitions []baseDiskTransition,
) error {

	var toDelete []persistence.Value
	var toUpsert []persistence.Value

	for _, t := range transitions {
		key := keyFromBaseDisk(t.state)

		oldFree := t.oldState != nil && t.oldState.hasFreeSlots()
		free := t.state.hasFreeSlots()

		switch {
		case oldFree && !free:
			toDelete = append(toDelete, key.structValue())
		case !oldFree && free:
			toUpsert = append(toUpsert, key.structValue())
		}
	}

	if len(toDelete) != 0 {
		_, err := tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $values as List<%v>;

			delete from free on
			select *
			from AS_TABLE($values)
		`, s.tablesPath, baseDiskKeyStructTypeString()),
			persistence.ValueParam("$values", persistence.ListValue(toDelete...)),
		)
		if err != nil {
			return err
		}
	}

	if len(toUpsert) == 0 {
		return nil
	}

	_, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $values as List<%v>;

		upsert into free
		select *
		from AS_TABLE($values)
	`, s.tablesPath, baseDiskKeyStructTypeString()),
		persistence.ValueParam("$values", persistence.ListValue(toUpsert...)),
	)
	return err
}

func (s *storageYDB) updateSchedulingTable(
	ctx context.Context,
	tx *persistence.Transaction,
	transitions []baseDiskTransition,
) error {

	var keys []persistence.Value
	for _, t := range transitions {
		if t.oldState != nil &&
			t.oldState.status != t.state.status &&
			t.oldState.status == baseDiskStatusScheduling {

			key := keyFromBaseDisk(t.state)
			keys = append(keys, key.structValue())

			logging.Info(
				ctx,
				"base disk %v will be removed from 'scheduling' table",
				t.state.id,
			)
		}
	}

	if len(keys) != 0 {
		_, err := tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $keys as List<%v>;

			delete from scheduling on
			select *
			from AS_TABLE($keys)
		`, s.tablesPath, baseDiskKeyStructTypeString()),
			persistence.ValueParam("$keys", persistence.ListValue(keys...)),
		)
		if err != nil {
			return err
		}
	}

	keys = nil
	for _, t := range transitions {
		if (t.oldState == nil || t.oldState.status != t.state.status) &&
			t.state.status == baseDiskStatusScheduling {

			key := keyFromBaseDisk(t.state)
			keys = append(keys, key.structValue())

			logging.Info(
				ctx,
				"base disk %v will be added to 'scheduling' table",
				t.state.id,
			)
		}
	}

	if len(keys) == 0 {
		return nil
	}

	_, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $keys as List<%v>;

		upsert into scheduling
		select *
		from AS_TABLE($keys)
	`, s.tablesPath, baseDiskKeyStructTypeString()),
		persistence.ValueParam("$keys", persistence.ListValue(keys...)),
	)
	return err
}

func (s *storageYDB) updateDeletingTable(
	ctx context.Context,
	tx *persistence.Transaction,
	transitions []baseDiskTransition,
) error {

	var values []persistence.Value

	for _, t := range transitions {
		if t.oldState != nil &&
			t.oldState.status != t.state.status &&
			t.oldState.status == baseDiskStatusDeleting {

			values = append(values, persistence.StructValue(
				persistence.StructFieldValue("base_disk_id", persistence.UTF8Value(t.state.id)),
			))
		}
	}

	if len(values) != 0 {
		_, err := tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $values as List<Struct<base_disk_id: Utf8>>;

			delete from deleting on
			select *
			from AS_TABLE($values)
		`, s.tablesPath),
			persistence.ValueParam("$values", persistence.ListValue(values...)),
		)
		if err != nil {
			return err
		}
	}

	values = nil
	for _, t := range transitions {
		if (t.oldState == nil || t.oldState.status != t.state.status) &&
			t.state.status == baseDiskStatusDeleting {

			values = append(values, persistence.StructValue(
				persistence.StructFieldValue("base_disk_id", persistence.UTF8Value(t.state.id)),
			))
		}
	}

	if len(values) == 0 {
		return nil
	}

	_, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $values as List<Struct<base_disk_id: Utf8>>;

		upsert into deleting
		select *
		from AS_TABLE($values)
	`, s.tablesPath),
		persistence.ValueParam("$values", persistence.ListValue(values...)),
	)
	return err
}

func (s *storageYDB) upsertSlots(
	ctx context.Context,
	tx *persistence.Transaction,
	slots []slot,
) error {

	var values []persistence.Value
	for _, slot := range slots {
		values = append(values, slot.structValue())
	}

	if len(values) == 0 {
		return nil
	}

	_, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $values as List<%v>;

		upsert into slots
		select *
		from AS_TABLE($values)
	`, s.tablesPath, slotStructTypeString()),
		persistence.ValueParam("$values", persistence.ListValue(values...)),
	)
	return err
}

func (s *storageYDB) upsertSlot(
	ctx context.Context,
	tx *persistence.Transaction,
	st slot,
) error {

	return s.upsertSlots(ctx, tx, []slot{st})
}

func (s *storageYDB) updateOverlayDiskIDs(
	ctx context.Context,
	tx *persistence.Transaction,
	transitions []slotTransition,
) error {

	var values []persistence.Value

	for _, t := range transitions {
		if t.oldState == nil {
			continue
		}

		if (t.oldState.status != t.state.status && t.state.status == slotStatusReleased) ||
			(t.oldState.baseDiskID != t.state.baseDiskID) {

			values = append(values, persistence.StructValue(
				persistence.StructFieldValue("base_disk_id", persistence.UTF8Value(t.oldState.baseDiskID)),
				persistence.StructFieldValue("overlay_disk_id", persistence.UTF8Value(t.state.overlayDiskID)),
			))
		}
	}

	if len(values) != 0 {
		_, err := tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $values as List<Struct<base_disk_id: Utf8, overlay_disk_id: Utf8>>;

			delete from overlay_disk_ids on
			select *
			from AS_TABLE($values)
		`, s.tablesPath),
			persistence.ValueParam("$values", persistence.ListValue(values...)),
		)
		if err != nil {
			return err
		}
	}

	values = nil
	for _, t := range transitions {
		if t.state.status == slotStatusAcquired &&
			(t.oldState == nil || t.oldState.baseDiskID != t.state.baseDiskID) {

			values = append(values, persistence.StructValue(
				persistence.StructFieldValue("base_disk_id", persistence.UTF8Value(t.state.baseDiskID)),
				persistence.StructFieldValue("overlay_disk_id", persistence.UTF8Value(t.state.overlayDiskID)),
			))
		}
	}

	if len(values) == 0 {
		return nil
	}

	_, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $values as List<Struct<base_disk_id: Utf8, overlay_disk_id: Utf8>>;

		upsert into overlay_disk_ids
		select *
		from AS_TABLE($values)
	`, s.tablesPath),
		persistence.ValueParam("$values", persistence.ListValue(values...)),
	)
	return err
}

func (s *storageYDB) updateSlots(
	ctx context.Context,
	tx *persistence.Transaction,
	transitions []slotTransition,
) error {

	var filtered []slotTransition
	for _, t := range transitions {
		if t.oldState == nil || *t.oldState != *t.state {
			filtered = append(filtered, t)
		}
	}
	transitions = filtered

	err := s.updateOverlayDiskIDs(ctx, tx, transitions)
	if err != nil {
		return err
	}

	var values []persistence.Value

	// Deletion from 'released' table is not permitted via slot transition.

	for _, t := range transitions {
		if (t.oldState == nil || t.oldState.status != t.state.status) &&
			t.state.status == slotStatusReleased {

			values = append(values, persistence.StructValue(
				persistence.StructFieldValue("released_at", persistence.TimestampValue(t.state.releasedAt)),
				persistence.StructFieldValue("overlay_disk_id", persistence.UTF8Value(t.state.overlayDiskID)),
			))
		}
	}

	if len(values) != 0 {
		_, err := tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $values as List<Struct<released_at: Timestamp, overlay_disk_id: Utf8>>;

			upsert into released
			select *
			from AS_TABLE($values)
		`, s.tablesPath),
			persistence.ValueParam("$values", persistence.ListValue(values...)),
		)
		if err != nil {
			return err
		}
	}

	values = nil
	for _, t := range transitions {
		values = append(values, t.state.structValue())
	}

	if len(values) == 0 {
		return nil
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $values as List<%v>;

		upsert into slots
		select *
		from AS_TABLE($values)
	`, s.tablesPath, slotStructTypeString()),
		persistence.ValueParam("$values", persistence.ListValue(values...)),
	)
	return err
}

func (s *storageYDB) updateSlot(
	ctx context.Context,
	tx *persistence.Transaction,
	transition slotTransition,
) error {

	return s.updateSlots(ctx, tx, []slotTransition{transition})
}

func (s *storageYDB) updateDeletedTable(
	ctx context.Context,
	tx *persistence.Transaction,
	transitions []baseDiskTransition,
) error {

	// Deletion from 'deleted' table is not permitted via base disk transition.

	var values []persistence.Value
	for _, t := range transitions {
		if (t.oldState == nil || t.oldState.status != t.state.status) &&
			t.state.status == baseDiskStatusDeleted {

			values = append(values, persistence.StructValue(
				persistence.StructFieldValue("deleted_at", persistence.TimestampValue(t.state.deletedAt)),
				persistence.StructFieldValue("base_disk_id", persistence.UTF8Value(t.state.id)),
			))
		}
	}

	if len(values) == 0 {
		return nil
	}

	_, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $values as List<Struct<deleted_at: Timestamp, base_disk_id: Utf8>>;

		upsert into deleted
		select *
		from AS_TABLE($values)
	`, s.tablesPath),
		persistence.ValueParam("$values", persistence.ListValue(values...)),
	)
	return err
}

func (s *storageYDB) makeSelectPoolsQuery() string {
	return fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $zone_id as Utf8;

		select *
		from pools
		where image_id = $image_id and zone_id = $zone_id
	`, s.tablesPath)
}

func (s *storageYDB) getPoolOrDefault(
	ctx context.Context,
	tx *persistence.Transaction,
	imageID string,
	zoneID string,
) (pool, error) {

	res, err := tx.Execute(
		ctx,
		s.makeSelectPoolsQuery(),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(zoneID)),
	)
	if err != nil {
		return pool{}, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return pool{
			imageID:   imageID,
			zoneID:    zoneID,
			status:    poolStatusReady,
			createdAt: time.Now(),
		}, nil
	}

	return scanPool(res)
}

func (s *storageYDB) applyBaseDiskInvariants(
	ctx context.Context,
	tx *persistence.Transaction,
	baseDiskTransitions []baseDiskTransition,
) ([]poolTransition, error) {

	poolTransitions := make(map[string]poolTransition)

	for _, baseDiskTransition := range baseDiskTransitions {
		baseDisk := baseDiskTransition.state

		imageID := baseDisk.imageID
		zoneID := baseDisk.zoneID
		key := imageID + zoneID

		t, ok := poolTransitions[key]
		if !ok {
			p, err := s.getPoolOrDefault(ctx, tx, imageID, zoneID)
			if err != nil {
				return nil, err
			}

			t = poolTransition{
				oldState: p,
				state:    p,
			}
		}

		if t.state.status == poolStatusDeleted {
			// Remove from deleted pool.
			baseDisk.fromPool = false
		}

		baseDisk.applyInvariants()

		logging.Info(
			ctx,
			"applying base disk transition from %+v to %+v",
			baseDiskTransition.oldState,
			baseDiskTransition.state,
		)

		action, err := computePoolAction(baseDiskTransition)
		if err != nil {
			return nil, err
		}

		logging.Info(
			ctx,
			"computed pool action is %+v",
			action,
		)

		action.apply(&t.state)

		poolTransitions[key] = t
	}

	var res []poolTransition
	for _, t := range poolTransitions {
		res = append(res, t)
	}

	return res, nil
}

func (s *storageYDB) updatePoolsTable(
	ctx context.Context,
	tx *persistence.Transaction,
	transitions []poolTransition,
) error {

	logging.Info(
		ctx,
		"applying pool transitions %+v",
		transitions,
	)

	var values []persistence.Value

	for _, t := range transitions {
		if t.oldState != t.state {
			values = append(values, t.state.structValue())
		}
	}

	if len(values) == 0 {
		return nil
	}

	_, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $pools as List<%v>;

		upsert into pools
		select *
		from AS_TABLE($pools)
	`, s.tablesPath, poolStructTypeString()),
		persistence.ValueParam("$pools", persistence.ListValue(values...)),
	)
	return err
}

func (s *storageYDB) updateBaseDisks(
	ctx context.Context,
	tx *persistence.Transaction,
	transitions []baseDiskTransition,
) error {

	var filtered []baseDiskTransition
	for _, t := range transitions {
		if t.oldState == nil || *t.oldState != *t.state {
			filtered = append(filtered, t)
		}
	}
	transitions = filtered

	poolTransitions, err := s.applyBaseDiskInvariants(ctx, tx, transitions)
	if err != nil {
		return err
	}

	err = s.updateSchedulingTable(ctx, tx, transitions)
	if err != nil {
		return err
	}

	err = s.updateDeletingTable(ctx, tx, transitions)
	if err != nil {
		return err
	}

	err = s.updateDeletedTable(ctx, tx, transitions)
	if err != nil {
		return err
	}

	err = s.updateFreeTable(ctx, tx, transitions)
	if err != nil {
		return err
	}

	err = s.updatePoolsTable(ctx, tx, poolTransitions)
	if err != nil {
		return err
	}

	var values []persistence.Value
	for _, t := range transitions {
		values = append(values, t.state.structValue())
	}

	if len(values) == 0 {
		return nil
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $base_disks as List<%v>;

		upsert into base_disks
		select *
		from AS_TABLE($base_disks)
	`, s.tablesPath, baseDiskStructTypeString()),
		persistence.ValueParam("$base_disks", persistence.ListValue(values...)),
	)
	return err
}

func (s *storageYDB) updateBaseDisk(
	ctx context.Context,
	tx *persistence.Transaction,
	transition baseDiskTransition,
) error {

	return s.updateBaseDisks(ctx, tx, []baseDiskTransition{transition})
}

func (s *storageYDB) updateBaseDisksAndSlots(
	ctx context.Context,
	tx *persistence.Transaction,
	baseDiskTransitions []baseDiskTransition,
	slotTransitions []slotTransition,
) error {

	err := s.updateBaseDisks(ctx, tx, baseDiskTransitions)
	if err != nil {
		return err
	}

	return s.updateSlots(ctx, tx, slotTransitions)
}

func (s *storageYDB) updateBaseDiskAndSlot(
	ctx context.Context,
	tx *persistence.Transaction,
	bt baseDiskTransition,
	st slotTransition,
) error {

	return s.updateBaseDisksAndSlots(
		ctx,
		tx,
		[]baseDiskTransition{bt},
		[]slotTransition{st},
	)
}

func (s *storageYDB) getPoolCapacity(
	ctx context.Context,
	tx *persistence.Transaction,
	imageID string,
	zoneID string,
) (capacity uint64, err error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $zone_id as Utf8;

		select capacity
		from configs
		where image_id = $image_id and zone_id = $zone_id
	`, s.tablesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(zoneID)),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	var rows int
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			rows++
			var c uint64
			err = res.ScanNamed(
				persistence.OptionalWithDefault("capacity", &c),
			)
			if err != nil {
				return 0, err
			}

			capacity += c
		}
	}

	if rows == 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return 0, err
		}

		err = errors.NewNonRetriableErrorf(
			"pool should be configured, imageID=%v, zoneID=%v",
			imageID,
			zoneID,
		)
		return 0, err
	}

	return capacity, nil
}

// Just an overloading for |isPoolConfigured|.
func (s *storageYDB) isPoolConfiguredTx(
	ctx context.Context,
	tx *persistence.Transaction,
	imageID string,
	zoneID string,
) (bool, error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $zone_id as Utf8;

		select count(*)
		from configs
		where image_id = $image_id and zone_id = $zone_id
	`, s.tablesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(zoneID)),
	)
	if err != nil {
		return false, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return false, nil
	}

	var count uint64
	err = res.ScanWithDefaults(&count)
	if err != nil {
		return false, err
	}

	return count != 0, nil
}

func (s *storageYDB) isPoolConfigured(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
	zoneID string,
) (bool, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $zone_id as Utf8;

		select count(*)
		from configs
		where image_id = $image_id and zone_id = $zone_id
	`, s.tablesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(zoneID)),
	)
	if err != nil {
		return false, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return false, nil
	}

	var count uint64
	err = res.ScanWithDefaults(&count)
	if err != nil {
		return false, err
	}

	return count != 0, nil
}

func (s *storageYDB) createPoolIfNecessary(
	ctx context.Context,
	tx *persistence.Transaction,
	imageID string,
	zoneID string,
) error {

	capacity, err := s.getPoolCapacity(ctx, tx, imageID, zoneID)
	if err != nil {
		return err
	}

	if capacity == 0 {
		// Zero capacity means that this pool should be created "on demand".
		_, err = tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $image_id as Utf8;
			declare $zone_id as Utf8;
			declare $kind as Int64;
			declare $capacity as Uint64;

			upsert into configs (image_id, zone_id, kind, capacity)
			values ($image_id, $zone_id, $kind, $capacity)
		`, s.tablesPath),
			persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
			persistence.ValueParam("$zone_id", persistence.UTF8Value(zoneID)),
			persistence.ValueParam("$kind", persistence.Int64Value(0)), // TODO: get rid of this param.
			persistence.ValueParam("$capacity", persistence.Uint64Value(1)),
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *storageYDB) acquireBaseDiskSlot(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
	st Slot,
) (BaseDisk, error) {

	zoneID := st.OverlayDisk.ZoneId
	overlayDiskID := st.OverlayDisk.DiskId

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return BaseDisk{}, err
	}
	defer tx.Rollback(ctx)

	acquiredSlot, err := s.getAcquiredSlot(ctx, tx, overlayDiskID)
	if err != nil {
		return BaseDisk{}, err
	}

	if acquiredSlot != nil {
		return s.acquireBaseDiskSlotIdempotent(ctx, tx, *acquiredSlot)
	}

	freeBaseDisks, err := s.getFreeBaseDisks(ctx, tx, imageID, zoneID)
	if err != nil {
		return BaseDisk{}, err
	}

	for _, baseDisk := range freeBaseDisks {
		if baseDisk.status != baseDiskStatusCreating &&
			baseDisk.status != baseDiskStatusReady {
			// Disk is not suitable for acquiring.
			continue
		}

		slot := &slot{
			overlayDiskID:   overlayDiskID,
			overlayDiskKind: st.OverlayDiskKind,
			overlayDiskSize: st.OverlayDiskSize,
			baseDiskID:      baseDisk.id,
			imageID:         baseDisk.imageID,
			zoneID:          zoneID,
			status:          slotStatusAcquired,
		}

		baseDiskOldState := baseDisk
		err = acquireUnitsAndSlots(ctx, tx, &baseDisk, slot)
		if err != nil {
			return BaseDisk{}, err
		}

		err = s.updateBaseDiskAndSlot(
			ctx,
			tx,
			baseDiskTransition{
				oldState: &baseDiskOldState,
				state:    &baseDisk,
			},
			slotTransition{
				oldState: nil,
				state:    slot,
			},
		)
		if err != nil {
			return BaseDisk{}, err
		}

		err = tx.Commit(ctx)
		if err != nil {
			return BaseDisk{}, err
		}

		logging.Info(
			ctx,
			"acquired slot %+v on baseDisk %+v",
			slot,
			baseDisk,
		)
		return baseDisk.toBaseDisk(), nil
	}

	err = s.createPoolIfNecessary(ctx, tx, imageID, zoneID)
	if err != nil {
		return BaseDisk{}, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return BaseDisk{}, err
	}

	// Should wait until target base disk is ready.
	return BaseDisk{}, errors.NewInterruptExecutionError()
}

func (s *storageYDB) releaseBaseDiskSlot(
	ctx context.Context,
	session *persistence.Session,
	overlayDisk *types.Disk,
) (BaseDisk, error) {

	overlayDiskID := overlayDisk.DiskId

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return BaseDisk{}, err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $overlay_disk_id as Utf8;

		select *
		from slots
		where overlay_disk_id = $overlay_disk_id
	`, s.tablesPath),
		persistence.ValueParam("$overlay_disk_id", persistence.UTF8Value(overlayDiskID)),
	)
	if err != nil {
		return BaseDisk{}, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		// Should be idempotent.
		return BaseDisk{}, tx.Commit(ctx)
	}

	slot, err := scanSlot(res)
	if err != nil {
		return BaseDisk{}, err
	}

	found, err := s.findBaseDisk(ctx, tx, slot.baseDiskID)
	if err != nil {
		return BaseDisk{}, err
	}

	if found == nil {
		// Should be idempotent.
		return BaseDisk{}, tx.Commit(ctx)
	}

	baseDisk := *found

	if slot.status == slotStatusReleased {
		err = tx.Commit(ctx)
		if err != nil {
			return BaseDisk{}, err
		}

		return baseDisk.toBaseDisk(), nil
	}

	slotOldState := slot
	slot.releasedAt = time.Now()
	slot.status = slotStatusReleased

	var slotTransitions []slotTransition
	slotTransitions = append(slotTransitions, slotTransition{
		oldState: &slotOldState,
		state:    &slot,
	})

	baseDiskOldState := baseDisk
	err = releaseUnitsAndSlots(ctx, tx, &baseDisk, slot)
	if err != nil {
		return BaseDisk{}, err
	}

	var baseDiskTransitions []baseDiskTransition
	baseDiskTransitions = append(baseDiskTransitions, baseDiskTransition{
		oldState: &baseDiskOldState,
		state:    &baseDisk,
	})

	if len(slot.targetBaseDiskID) != 0 {
		baseDisk, err := s.getBaseDisk(ctx, tx, slot.targetBaseDiskID)
		if err != nil {
			return BaseDisk{}, err
		}

		baseDiskOldState := baseDisk
		err = releaseTargetUnitsAndSlots(ctx, tx, &baseDisk, slot)
		if err != nil {
			return BaseDisk{}, err
		}

		baseDiskTransitions = append(baseDiskTransitions, baseDiskTransition{
			oldState: &baseDiskOldState,
			state:    &baseDisk,
		})
	}

	err = s.updateBaseDisksAndSlots(ctx, tx, baseDiskTransitions, slotTransitions)
	if err != nil {
		return BaseDisk{}, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return BaseDisk{}, err
	}

	logging.Info(
		ctx,
		"released slot %+v on baseDisk %+v",
		slot,
		baseDisk,
	)
	return baseDisk.toBaseDisk(), nil
}

func (s *storageYDB) getAcquiredSlot(
	ctx context.Context,
	tx *persistence.Transaction,
	overlayDiskID string,
) (*slot, error) {

	slots, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $overlay_disk_id as Utf8;

		select *
		from slots
		where overlay_disk_id = $overlay_disk_id
	`, s.tablesPath),
		persistence.ValueParam("$overlay_disk_id", persistence.UTF8Value(overlayDiskID)),
	)
	if err != nil {
		return nil, err
	}
	defer slots.Close()

	if slots.NextResultSet(ctx) && slots.NextRow() {
		scannedSlot, err := scanSlot(slots)
		if err != nil {
			return nil, err
		}

		return &scannedSlot, err
	}

	return nil, nil
}

func (s *storageYDB) relocateOverlayDiskTx(
	ctx context.Context,
	tx *persistence.Transaction,
	overlayDisk *types.Disk,
	targetZoneID string,
) (RebaseInfo, error) {

	overlayDiskID := overlayDisk.DiskId

	acquiredSlot, err := s.getAcquiredSlot(ctx, tx, overlayDiskID)
	if err != nil {
		return RebaseInfo{}, err
	}

	if acquiredSlot == nil {
		return RebaseInfo{}, nil
	}

	if len(acquiredSlot.targetBaseDiskID) != 0 {
		if len(acquiredSlot.targetZoneID) == 0 {
			err := tx.Commit(ctx)
			if err != nil {
				return RebaseInfo{}, err
			}

			// Should wait until end of rebase within current zone.
			return RebaseInfo{}, errors.NewInterruptExecutionError()
		} else if acquiredSlot.targetZoneID == targetZoneID {
			rebaseInfo := RebaseInfo{
				OverlayDisk:      overlayDisk,
				BaseDiskID:       acquiredSlot.baseDiskID,
				TargetZoneID:     targetZoneID,
				TargetBaseDiskID: acquiredSlot.targetBaseDiskID,
				SlotGeneration:   acquiredSlot.generation,
			}

			err = s.overlayDiskRebasingTx(ctx, tx, rebaseInfo, *acquiredSlot)
			if err != nil {
				return RebaseInfo{}, err
			}

			logging.Info(ctx, "Overlay disk relocating with RebaseInfo %+v", rebaseInfo)
			return rebaseInfo, nil
		}
	}

	// Abort previous relocation.
	if len(acquiredSlot.targetBaseDiskID) != 0 {
		baseDisk, err := s.getBaseDisk(ctx, tx, acquiredSlot.targetBaseDiskID)
		if err != nil {
			return RebaseInfo{}, err
		}

		baseDiskOldState := baseDisk
		err = releaseTargetUnitsAndSlots(ctx, tx, &baseDisk, *acquiredSlot)
		if err != nil {
			return RebaseInfo{}, err
		}

		acquiredSlotOldState := *acquiredSlot

		acquiredSlot.targetZoneID = ""
		acquiredSlot.targetBaseDiskID = ""
		acquiredSlot.targetAllottedSlots = 0
		acquiredSlot.targetAllottedUnits = 0

		err = s.updateBaseDiskAndSlot(
			ctx,
			tx,
			baseDiskTransition{
				oldState: &baseDiskOldState,
				state:    &baseDisk,
			},
			slotTransition{
				oldState: &acquiredSlotOldState,
				state:    acquiredSlot,
			},
		)
		if err != nil {
			return RebaseInfo{}, err
		}

		err = tx.Commit(ctx)
		if err != nil {
			return RebaseInfo{}, err
		}

		return RebaseInfo{}, errors.NewInterruptExecutionError()
	}

	imageID := acquiredSlot.imageID
	isPoolConfigured, err := s.isPoolConfiguredTx(ctx, tx, imageID, targetZoneID)
	if err != nil {
		return RebaseInfo{}, err
	}

	if !isPoolConfigured {
		return RebaseInfo{}, nil
	}

	freeBaseDisks, err := s.getFreeBaseDisks(ctx, tx, imageID, targetZoneID)
	if err != nil {
		return RebaseInfo{}, err
	}

	for _, baseDisk := range freeBaseDisks {
		if baseDisk.status != baseDiskStatusCreating &&
			baseDisk.status != baseDiskStatusReady {
			// Disk is not suitable for acquiring.
			continue
		}

		acquiredSlot.generation += 1

		rebaseInfo := RebaseInfo{
			OverlayDisk:      overlayDisk,
			BaseDiskID:       acquiredSlot.baseDiskID,
			TargetZoneID:     targetZoneID,
			TargetBaseDiskID: baseDisk.id,
			SlotGeneration:   acquiredSlot.generation,
		}

		err = s.overlayDiskRebasingTx(ctx, tx, rebaseInfo, *acquiredSlot)
		if err != nil {
			return RebaseInfo{}, err
		}

		logging.Info(ctx, "Overlay disk relocating with RebaseInfo %+v", rebaseInfo)
		return rebaseInfo, nil
	}

	err = s.createPoolIfNecessary(ctx, tx, imageID, targetZoneID)
	if err != nil {
		return RebaseInfo{}, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return RebaseInfo{}, err
	}

	// Should wait until target base disk is ready.
	return RebaseInfo{}, errors.NewInterruptExecutionError()
}

func (s *storageYDB) overlayDiskRebasingTx(
	ctx context.Context,
	tx *persistence.Transaction,
	info RebaseInfo,
	slot slot,
) error {

	if slot.status == slotStatusReleased {
		err := tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"slot with id %v is already released",
			info.OverlayDisk.DiskId,
		)
	}

	if slot.generation != info.SlotGeneration {
		err := tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"using wrong generation, expected=%v, actual=%v",
			info.SlotGeneration,
			slot.generation,
		)
	}

	if len(slot.targetBaseDiskID) != 0 && slot.targetBaseDiskID != info.TargetBaseDiskID {
		err := tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"another rebase or relocate is in progress for slot %+v",
			slot,
		)
	}

	found, err := s.getBaseDisk(ctx, tx, info.TargetBaseDiskID)
	if err != nil {
		return err
	}

	if found.isDoomed() {
		slotOldState := slot

		// Abort rebasing.
		slot.targetBaseDiskID = ""
		slot.targetAllottedSlots = 0
		slot.targetAllottedUnits = 0

		baseDiskOldState := found
		err = releaseTargetUnitsAndSlots(ctx, tx, &found, slotOldState)
		if err != nil {
			return err
		}

		err = s.updateBaseDiskAndSlot(
			ctx,
			tx,
			baseDiskTransition{
				oldState: &baseDiskOldState,
				state:    &found,
			},
			slotTransition{
				oldState: &slotOldState,
				state:    &slot,
			},
		)
		if err != nil {
			return err
		}

		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"rebase is aborted, target base disk with id %v is already deleting/deleted",
			info.TargetBaseDiskID,
		)
	}

	baseDisk := found

	if slot.baseDiskID == info.TargetBaseDiskID {
		// Nothing to do.
		return nil
	}

	if slot.targetBaseDiskID == info.TargetBaseDiskID {
		// Target units are already acquired.

		if baseDisk.status != baseDiskStatusReady {
			err = tx.Commit(ctx)
			if err != nil {
				return err
			}

			// Should wait until target base disk is ready.
			return errors.NewInterruptExecutionError()
		}

		return nil
	}

	baseDiskOldState := baseDisk
	slotOldState := slot

	err = acquireTargetUnitsAndSlots(ctx, tx, &baseDisk, &slot)
	if err != nil {
		return err
	}

	if len(info.TargetZoneID) != 0 {
		slot.targetZoneID = info.TargetZoneID
	}

	slot.targetBaseDiskID = info.TargetBaseDiskID

	err = s.updateBaseDiskAndSlot(
		ctx,
		tx,
		baseDiskTransition{
			oldState: &baseDiskOldState,
			state:    &baseDisk,
		},
		slotTransition{
			oldState: &slotOldState,
			state:    &slot,
		},
	)
	if err != nil {
		return err
	}

	if baseDisk.status != baseDiskStatusReady {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		// Should wait until target base disk is ready.
		return errors.NewInterruptExecutionError()
	}

	return nil
}

func (s *storageYDB) overlayDiskRebasedTx(
	ctx context.Context,
	tx *persistence.Transaction,
	info RebaseInfo,
) error {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $overlay_disk_id as Utf8;

		select *
		from slots
		where overlay_disk_id = $overlay_disk_id
	`, s.tablesPath),
		persistence.ValueParam(
			"$overlay_disk_id",
			persistence.UTF8Value(info.OverlayDisk.DiskId),
		),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"failed to find slot with id %v",
			info.OverlayDisk.DiskId,
		)
	}

	slot, err := scanSlot(res)
	if err != nil {
		return err
	}

	if slot.status == slotStatusReleased {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"slot with id %v is already released",
			info.OverlayDisk.DiskId,
		)
	}

	if slot.baseDiskID == info.TargetBaseDiskID {
		// Nothing to do.
		return nil
	}

	if slot.generation != info.SlotGeneration {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"using wrong generation, expected=%v, actual=%v",
			info.SlotGeneration,
			slot.generation,
		)
	}

	if slot.targetBaseDiskID != info.TargetBaseDiskID {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"another rebase or relocate is in progress for slot %+v with info %v",
			slot,
			info,
		)
	}

	baseDisk, err := s.getBaseDisk(ctx, tx, slot.baseDiskID)
	if err != nil {
		return err
	}

	baseDiskOldState := baseDisk
	err = releaseUnitsAndSlots(ctx, tx, &baseDisk, slot)
	if err != nil {
		return err
	}

	slotOldState := slot

	if len(slot.targetZoneID) != 0 {
		slot.zoneID = slot.targetZoneID
	}

	slot.baseDiskID = slot.targetBaseDiskID
	slot.allottedSlots = slot.targetAllottedSlots
	slot.allottedUnits = slot.targetAllottedUnits

	// Finish rebasing.
	slot.targetZoneID = ""
	slot.targetBaseDiskID = ""
	slot.targetAllottedSlots = 0
	slot.targetAllottedUnits = 0

	return s.updateBaseDiskAndSlot(
		ctx,
		tx,
		baseDiskTransition{
			oldState: &baseDiskOldState,
			state:    &baseDisk,
		},
		slotTransition{
			oldState: &slotOldState,
			state:    &slot,
		},
	)
}

func (s *storageYDB) overlayDiskRebasing(
	ctx context.Context,
	session *persistence.Session,
	info RebaseInfo,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	slot, err := s.getAcquiredSlot(ctx, tx, info.OverlayDisk.DiskId)
	if err != nil {
		return err
	}

	if slot == nil {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"failed to find slot with id %v",
			info.OverlayDisk.DiskId,
		)
	}

	err = s.overlayDiskRebasingTx(ctx, tx, info, *slot)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	logging.Info(ctx, "Overlay disk rebasing for RebaseInfo %+v", info)
	return nil
}

func (s *storageYDB) overlayDiskRebased(
	ctx context.Context,
	session *persistence.Session,
	info RebaseInfo,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	err = s.overlayDiskRebasedTx(ctx, tx, info)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	logging.Info(ctx, "Overlay disk rebased for RebaseInfo %+v", info)
	return nil
}

func (s *storageYDB) baseDiskCreated(
	ctx context.Context,
	session *persistence.Session,
	baseDisk BaseDisk,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	found, err := s.getBaseDisk(ctx, tx, baseDisk.ID)
	if err != nil {
		return err
	}

	if found.isDoomed() {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"disk %+v is already deleting/deleted",
			found,
		)
	}

	if found.status == baseDiskStatusReady {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	updated := found
	updated.status = baseDiskStatusReady

	err = s.updateBaseDisk(ctx, tx, baseDiskTransition{
		oldState: &found,
		state:    &updated,
	})
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) baseDiskCreationFailed(
	ctx context.Context,
	session *persistence.Session,
	lookup BaseDisk,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from base_disks
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(lookup.ID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	found, err := scanBaseDisk(res)
	if err != nil {
		return err
	}

	if found.isDoomed() {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	if found.status == baseDiskStatusReady {
		// In fact, create was successful. Nothing to do.
		return tx.Commit(ctx)
	}

	updated := found
	updated.status = baseDiskStatusCreationFailed

	err = s.updateBaseDisk(ctx, tx, baseDiskTransition{
		oldState: &found,
		state:    &updated,
	})
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) getPoolConfig(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
	zoneID string,
) (*poolConfig, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $zone_id as Utf8;

		select *
		from configs
		where image_id = $image_id and zone_id = $zone_id
	`, s.tablesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(zoneID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return nil, nil
	}

	config, err := scanPoolConfig(res)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func (s *storageYDB) getPoolConfigs(
	ctx context.Context,
	session *persistence.Session,
) (configs []poolConfig, err error) {

	logging.Info(ctx, "getting pool configs")

	defer session.DisksStatCall(
		ctx,
		"storageYDB/getPoolConfigs",
	)(&err)

	res, err := session.StreamExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";

		select *
		from configs
	`, s.tablesPath,
	))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	m := make(map[string]*poolConfig)

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			config, err := scanPoolConfig(res)
			if err != nil {
				return nil, err
			}

			key := config.imageID + config.zoneID

			// NBS-4007: don't use 'kind' key of 'configs' table.
			if c, ok := m[key]; ok {
				// Just accumulate capacities of all pool kinds.
				// TODO: Do something better.
				config.capacity += c.capacity
			}
			m[key] = &config
		}
	}

	// NOTE: always check stream query result after iteration.
	err = res.Err()
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	for _, config := range m {
		configs = append(configs, *config)
	}

	return configs, nil
}

func (s *storageYDB) getBaseDisksScheduling(
	ctx context.Context,
	session *persistence.Session,
) (baseDisks []BaseDisk, err error) {

	logging.Info(ctx, "getting base disks for scheduling")

	defer session.DisksStatCall(
		ctx,
		"storageYDB/getBaseDisksScheduling",
	)(&err)

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";

		select *
		from scheduling
	`, s.tablesPath,
	))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var ids []string
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var id string
			err = res.ScanNamed(
				persistence.OptionalWithDefault("base_disk_id", &id),
			)
			if err != nil {
				return nil, err
			}

			ids = append(ids, id)
		}
	}

	logging.Info(ctx, "finding base disks from %v disks", len(ids))

	found, err := s.findBaseDisks(ctx, session, ids)
	if err != nil {
		return nil, err
	}

	for _, baseDisk := range found {
		baseDisks = append(baseDisks, baseDisk.toBaseDisk())
	}

	return baseDisks, nil
}

func (s *storageYDB) takeBaseDisksToScheduleForPool(
	ctx context.Context,
	session *persistence.Session,
	config poolConfig,
) (res []BaseDisk, err error) {

	logging.Info(ctx, "started taking base disks to schedule for pool %v", config)

	defer session.DisksStatCall(
		ctx,
		"storageYDB/getPoolConfigs",
	)(&err)

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Check that config is still present (NBS-2171).
	configured, err := s.isPoolConfiguredTx(
		ctx,
		tx,
		config.imageID,
		config.zoneID,
	)
	if err != nil {
		return nil, err
	}

	if !configured {
		// Nothing to schedule.
		return nil, tx.Commit(ctx)
	}

	pool, err := s.getPoolOrDefault(ctx, tx, config.imageID, config.zoneID)
	if err != nil {
		return nil, err
	}

	pool.status = poolStatusReady

	if pool.size >= config.capacity {
		// Pool is already full.
		return nil, tx.Commit(ctx)
	}

	baseDiskTemplate := s.generateBaseDisk(
		config.imageID,
		config.zoneID,
		config.imageSize,
		nil, // srcDisk
	)

	// size and capacity are measured in slots, not base disks.
	wantToCreate := config.capacity - pool.size
	// TODO: introduce new capacity param that is measured in units
	wantToCreate = divideWithRoundingUp(
		wantToCreate,
		baseDiskTemplate.freeSlots(),
	)

	if s.maxBaseDisksInflight <= pool.baseDisksInflight {
		// Limit number of base disks being currently created.
		return nil, tx.Commit(ctx)
	}

	maxPermittedToCreate := s.maxBaseDisksInflight - pool.baseDisksInflight
	willCreate := min(maxPermittedToCreate, wantToCreate)

	pool.size += willCreate * baseDiskTemplate.freeSlots()
	pool.freeUnits += willCreate * baseDiskTemplate.freeUnits()
	pool.baseDisksInflight += willCreate

	var newBaseDisks []baseDisk
	for i := uint64(0); i < willCreate; i++ {
		// All base disks should have the same params.
		baseDiskTemplate.id = generateBaseDiskID(s.baseDiskIDPrefix)
		newBaseDisks = append(newBaseDisks, baseDiskTemplate)

		logging.Info(
			ctx,
			"generated base disk: %+v",
			baseDiskTemplate,
		)
	}

	var transitions []baseDiskTransition
	for i := 0; i < len(newBaseDisks); i++ {
		transitions = append(transitions, baseDiskTransition{
			oldState: nil,
			state:    &newBaseDisks[i],
		})
	}

	err = s.updateSchedulingTable(ctx, tx, transitions)
	if err != nil {
		return nil, err
	}

	err = s.updateFreeTable(ctx, tx, transitions)
	if err != nil {
		return nil, err
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $pool as List<%v>;

		upsert into pools
		select *
		from AS_TABLE($pool)
	`, s.tablesPath, poolStructTypeString()),
		persistence.ValueParam("$pool", persistence.ListValue(pool.structValue())),
	)
	if err != nil {
		return nil, err
	}

	var values []persistence.Value
	for _, baseDisk := range newBaseDisks {
		values = append(values, baseDisk.structValue())
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $base_disks as List<%v>;

		upsert into base_disks
		select *
		from AS_TABLE($base_disks)
	`, s.tablesPath, baseDiskStructTypeString()),
		persistence.ValueParam("$base_disks", persistence.ListValue(values...)),
	)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	for _, baseDisk := range newBaseDisks {
		res = append(res, baseDisk.toBaseDisk())
	}

	logging.Info(ctx, "Selected %v disks for pool %v", len(res), config)

	return res, nil
}

func (s *storageYDB) takeBaseDisksToSchedule(
	ctx context.Context,
	session *persistence.Session,
) ([]BaseDisk, error) {

	logging.Info(ctx, "started takeBaseDisksToSchedule")

	configs, err := s.getPoolConfigs(ctx, session)
	if err != nil {
		return nil, err
	}

	scheduling, err := s.getBaseDisksScheduling(ctx, session)
	if err != nil {
		return nil, err
	}

	logging.Info(ctx, "got %v pool configs and %v disks", len(configs), len(scheduling))

	var baseDisks []BaseDisk

	for _, disk := range scheduling {
		if disk.SrcDisk != nil {
			baseDisks = append(baseDisks, disk)
		}
	}

	var toSchedule []poolConfig

	for _, config := range configs {
		alreadyScheduling := false

		for _, disk := range scheduling {
			if disk.ImageID == config.imageID && disk.ZoneID == config.zoneID {
				alreadyScheduling = true
				baseDisks = append(baseDisks, disk)
			}
		}

		if alreadyScheduling {
			// We have something to schedule, so skip this pool.
			continue
		}

		toSchedule = append(toSchedule, config)
	}

	startIndex := 0
	for startIndex < len(toSchedule) {
		endIndex := startIndex + s.takeBaseDisksToScheduleParallelism
		if endIndex > len(toSchedule) {
			endIndex = len(toSchedule)
		}

		group, ctx := errgroup.WithContext(ctx)
		c := make(chan []BaseDisk)

		for i := startIndex; i < endIndex; i++ {
			config := toSchedule[i]

			group.Go(func() error {
				return s.db.Execute(
					ctx,
					func(ctx context.Context, session *persistence.Session) (err error) {
						var disks []BaseDisk
						disks, err = s.takeBaseDisksToScheduleForPool(
							ctx,
							session,
							config,
						)
						if err != nil {
							return err
						}

						select {
						case c <- disks:
						case <-ctx.Done():
							return ctx.Err()
						}
						return nil
					},
				)
			})
		}
		go func() {
			_ = group.Wait()
			close(c)
		}()

		for disks := range c {
			baseDisks = append(baseDisks, disks...)
		}

		err := group.Wait()
		if err != nil {
			return nil, err
		}

		startIndex = endIndex
	}

	logging.Info(ctx, "finished takeBaseDisksToSchedule")

	return baseDisks, nil
}

func (s *storageYDB) getReadyPoolInfos(
	ctx context.Context,
	session *persistence.Session,
) ([]PoolInfo, error) {

	configs, err := s.getPoolConfigs(ctx, session)
	if err != nil {
		return nil, err
	}

	var poolInfos []PoolInfo
	for _, config := range configs {
		err := func() error {
			res, err := session.ExecuteRO(
				ctx,
				s.makeSelectPoolsQuery(),
				persistence.ValueParam("$image_id", persistence.UTF8Value(config.imageID)),
				persistence.ValueParam("$zone_id", persistence.UTF8Value(config.zoneID)),
			)
			if err != nil {
				return err
			}
			defer res.Close()

			if !res.NextResultSet(ctx) || !res.NextRow() {
				return nil
			}

			pool, err := scanPool(res)
			if err != nil {
				return err
			}

			if pool.status != poolStatusReady {
				return nil
			}

			poolInfos = append(poolInfos, PoolInfo{
				ImageID:       config.imageID,
				ZoneID:        config.zoneID,
				FreeUnits:     pool.freeUnits,
				AcquiredUnits: pool.acquiredUnits,
				Capacity:      uint32(config.capacity),
				ImageSize:     config.imageSize,
				CreatedAt:     pool.createdAt,
			})

			return nil
		}()

		if err != nil {
			return nil, err
		}
	}

	return poolInfos, nil
}

func (s *storageYDB) baseDiskScheduled(
	ctx context.Context,
	session *persistence.Session,
	baseDisk BaseDisk,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	found, err := s.getBaseDisk(ctx, tx, baseDisk.ID)
	if err != nil {
		return err
	}

	if found.status != baseDiskStatusScheduling {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	updated := found
	updated.createTaskID = baseDisk.CreateTaskID
	updated.status = baseDiskStatusCreating

	err = s.updateBaseDisk(ctx, tx, baseDiskTransition{
		oldState: &found,
		state:    &updated,
	})
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) baseDisksScheduled(
	ctx context.Context,
	session *persistence.Session,
	baseDisks []BaseDisk,
) error {

	for _, disk := range baseDisks {
		err := s.baseDiskScheduled(ctx, session, disk)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *storageYDB) configurePool(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
	zoneID string,
	capacity uint32,
	imageSize uint64,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	p, err := s.getPoolOrDefault(ctx, tx, imageID, zoneID)
	if err != nil {
		return err
	}

	if p.status == poolStatusDeleted {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewSilentNonRetriableErrorf(
			"can't configure already deleted pool, imageID=%v, zoneID=%v",
			imageID,
			zoneID,
		)
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $zone_id as Utf8;
		declare $kind as Int64;
		declare $capacity as Uint64;
		declare $image_size as Uint64;

		upsert into configs (image_id, zone_id, kind, capacity, image_size)
		values ($image_id, $zone_id, $kind, $capacity, $image_size)
	`, s.tablesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(zoneID)),
		persistence.ValueParam("$kind", persistence.Int64Value(0)), // TODO: get rid of this param.
		persistence.ValueParam("$capacity", persistence.Uint64Value(uint64(capacity))),
		persistence.ValueParam("$image_size", persistence.Uint64Value(imageSize)),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) removeBaseDisksFromPool(
	ctx context.Context,
	tx *persistence.Transaction,
	toDelete []baseDisk,
) error {

	var baseDiskTransitions []baseDiskTransition
	for i := 0; i < len(toDelete); i++ {
		baseDiskOldState := toDelete[i]
		toDelete[i].fromPool = false

		baseDiskTransitions = append(baseDiskTransitions, baseDiskTransition{
			oldState: &baseDiskOldState,
			state:    &toDelete[i],
		})
	}

	return s.updateBaseDisks(ctx, tx, baseDiskTransitions)
}

func (s *storageYDB) deletePool(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
	zoneID string,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $zone_id as Utf8;

		select *
		from pools
		where image_id = $image_id and zone_id = $zone_id
	`, s.tablesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(zoneID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		// Nothing to do.
		return tx.Commit(ctx)
	}

	p, err := scanPool(res)
	if err != nil {
		return err
	}

	if p.status == poolStatusDeleted {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	if len(p.lockID) != 0 {
		err := tx.Commit(ctx)
		if err != nil {
			return err
		}

		// Pool is locked.
		return errors.NewInterruptExecutionError()
	}

	freeBaseDisks, err := s.getFreeBaseDisks(ctx, tx, p.imageID, p.zoneID)
	if err != nil {
		return err
	}

	var toDelete []baseDisk
	for _, baseDisk := range freeBaseDisks {
		if baseDisk.activeSlots == 0 {
			toDelete = append(toDelete, baseDisk)
		}
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $zone_id as Utf8;

		delete from configs where
		image_id = $image_id and zone_id = $zone_id;

		delete from free where
		image_id = $image_id and zone_id = $zone_id;
	`, s.tablesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(p.imageID)),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(p.zoneID)),
	)
	if err != nil {
		return err
	}

	updated := p
	updated.status = poolStatusDeleted

	logging.Info(ctx, "applying pool transition from %+v to %+v", p, updated)

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $pool as List<%v>;

		upsert into pools
		select *
		from AS_TABLE($pool)
	`, s.tablesPath, poolStructTypeString()),
		persistence.ValueParam("$pool", persistence.ListValue(updated.structValue())),
	)
	if err != nil {
		return err
	}

	if len(toDelete) == 0 {
		return tx.Commit(ctx)
	}

	err = s.removeBaseDisksFromPool(ctx, tx, toDelete)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) imageDeleting(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
) error {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $status as Int64;

		select *
		from pools
		where image_id = $image_id and status = $status
	`, s.tablesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$status", persistence.Int64Value(int64(poolStatusReady))),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			pool, err := scanPool(res)
			if err != nil {
				return err
			}

			err = s.deletePool(ctx, session, pool.imageID, pool.zoneID)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *storageYDB) getBaseDisksToDelete(
	ctx context.Context,
	session *persistence.Session,
	limit uint64,
) (baseDisks []BaseDisk, err error) {

	deleting, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $limit as Uint64;

		select *
		from deleting
		limit $limit
	`, s.tablesPath),
		persistence.ValueParam("$limit", persistence.Uint64Value(limit)),
	)
	if err != nil {
		return nil, err
	}
	defer deleting.Close()

	for deleting.NextResultSet(ctx) {
		for deleting.NextRow() {
			var (
				baseDiskID string
				res        persistence.Result
			)
			err = deleting.ScanNamed(
				persistence.OptionalWithDefault("base_disk_id", &baseDiskID),
			)
			if err != nil {
				return nil, err
			}

			res, err = session.ExecuteRO(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				declare $id as Utf8;
				declare $status as Int64;

				select *
				from base_disks
				where id = $id and status = $status
			`, s.tablesPath),
				persistence.ValueParam("$id", persistence.UTF8Value(baseDiskID)),
				persistence.ValueParam("$status", persistence.Int64Value(int64(baseDiskStatusDeleting))),
			)
			if err != nil {
				return nil, err
			}
			defer res.Close() // TODO: dont use defer in loop!!!

			if !res.NextResultSet(ctx) || !res.NextRow() {
				return nil, nil
			}

			var baseDisk baseDisk
			baseDisk, err := scanBaseDisk(res)
			if err != nil {
				return nil, err
			}

			baseDisks = append(baseDisks, baseDisk.toBaseDisk())
		}
	}

	return baseDisks, nil
}

func (s *storageYDB) baseDiskDeleted(
	ctx context.Context,
	session *persistence.Session,
	lookup BaseDisk,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from base_disks
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(lookup.ID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	found, err := scanBaseDisk(res)
	if err != nil {
		return err
	}

	if found.status == baseDiskStatusDeleted {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	if found.status != baseDiskStatusDeleting {
		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"internal inconsistency: base disk has invalid status %v",
			found.status,
		)
	}

	updated := found
	updated.deletedAt = time.Now()
	updated.status = baseDiskStatusDeleted

	err = s.updateBaseDisk(ctx, tx, baseDiskTransition{
		oldState: &found,
		state:    &updated,
	})
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) baseDisksDeleted(
	ctx context.Context,
	session *persistence.Session,
	baseDisks []BaseDisk,
) error {

	for _, disk := range baseDisks {
		err := s.baseDiskDeleted(ctx, session, disk)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *storageYDB) clearDeletedBaseDisks(
	ctx context.Context,
	session *persistence.Session,
	deletedBefore time.Time,
	limit int,
) error {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $deleted_before as Timestamp;
		declare $limit as Uint64;

		select *
		from deleted
		where deleted_at < $deleted_before
		limit $limit
	`, s.tablesPath),
		persistence.ValueParam("$deleted_before", persistence.TimestampValue(deletedBefore)),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var (
				deletedAt  time.Time
				baseDiskID string
			)
			err = res.ScanNamed(
				persistence.OptionalWithDefault("deleted_at", &deletedAt),
				persistence.OptionalWithDefault("base_disk_id", &baseDiskID),
			)
			if err != nil {
				return err
			}

			_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				declare $deleted_at as Timestamp;
				declare $base_disk_id as Utf8;
				declare $status as Int64;

				delete from base_disks
				where id = $base_disk_id and status = $status;

				delete from deleted
				where deleted_at = $deleted_at and base_disk_id = $base_disk_id
			`, s.tablesPath),
				persistence.ValueParam("$deleted_at", persistence.TimestampValue(deletedAt)),
				persistence.ValueParam("$base_disk_id", persistence.UTF8Value(baseDiskID)),
				persistence.ValueParam("$status", persistence.Int64Value(int64(baseDiskStatusDeleted))),
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *storageYDB) clearReleasedSlots(
	ctx context.Context,
	session *persistence.Session,
	releasedBefore time.Time,
	limit int,
) error {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $released_before as Timestamp;
		declare $limit as Uint64;

		select *
		from released
		where released_at < $released_before
		limit $limit
	`, s.tablesPath),
		persistence.ValueParam("$released_before", persistence.TimestampValue(releasedBefore)),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var (
				releasedAt    time.Time
				overlayDiskID string
			)
			err = res.ScanNamed(
				persistence.OptionalWithDefault("released_at", &releasedAt),
				persistence.OptionalWithDefault("overlay_disk_id", &overlayDiskID),
			)
			if err != nil {
				return err
			}

			_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				declare $released_at as Timestamp;
				declare $overlay_disk_id as Utf8;
				declare $status as Int64;

				delete from slots
				where overlay_disk_id = $overlay_disk_id and status = $status;

				delete from released
				where released_at = $released_at and overlay_disk_id = $overlay_disk_id
			`, s.tablesPath),
				persistence.ValueParam("$released_at", persistence.TimestampValue(releasedAt)),
				persistence.ValueParam("$overlay_disk_id", persistence.UTF8Value(overlayDiskID)),
				persistence.ValueParam("$status", persistence.Int64Value(int64(slotStatusReleased))),
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *storageYDB) getAcquiredSlots(
	ctx context.Context,
	tx *persistence.Transaction,
	baseDiskID string,
) ([]slot, error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $base_disk_id as Utf8;

		select overlay_disk_id
		from overlay_disk_ids
		where base_disk_id = $base_disk_id
	`, s.tablesPath),
		persistence.ValueParam("$base_disk_id", persistence.UTF8Value(baseDiskID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var overlayDiskIDs []persistence.Value
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var id string
			err = res.ScanNamed(
				persistence.OptionalWithDefault("overlay_disk_id", &id),
			)
			if err != nil {
				return nil, err
			}

			overlayDiskIDs = append(overlayDiskIDs, persistence.UTF8Value(id))
		}
	}

	if len(overlayDiskIDs) == 0 {
		return nil, nil
	}

	res, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $overlay_disk_ids as List<Utf8>;

		select *
		from slots
		where overlay_disk_id in $overlay_disk_ids
	`, s.tablesPath),
		persistence.ValueParam("$overlay_disk_ids", persistence.ListValue(overlayDiskIDs...)),
	)
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

func (s *storageYDB) getFreeBaseDisks(
	ctx context.Context,
	tx *persistence.Transaction,
	imageID string,
	zoneID string,
) ([]baseDisk, error) {

	// TODO: use join here.
	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $zone_id as Utf8;

		select *
		from free
		where image_id = $image_id and zone_id = $zone_id
	`, s.tablesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(zoneID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var ids []string
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var id string
			err = res.ScanNamed(
				persistence.OptionalWithDefault("base_disk_id", &id),
			)
			if err != nil {
				return nil, err
			}

			ids = append(ids, id)
		}
	}

	return s.findBaseDisksTx(ctx, tx, ids)
}

func (s *storageYDB) getNonRetiringBaseDisks(
	ctx context.Context,
	tx *persistence.Transaction,
	imageID string,
	zoneID string,
) ([]baseDisk, error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $zone_id as Utf8;
		declare $status as Int64;

		select *
		from base_disks
		where image_id = $image_id
			and zone_id = $zone_id
			and retiring = false
			and status = $status
	`, s.tablesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(zoneID)),
		persistence.ValueParam("$status", persistence.Int64Value(int64(baseDiskStatusReady))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	baseDisks, err := scanBaseDisks(ctx, res)
	if err != nil {
		return nil, err
	}

	return baseDisks, err
}

func (s *storageYDB) retireBaseDisk(
	ctx context.Context,
	session *persistence.Session,
	baseDiskID string,
	srcDisk *types.Disk,
	useImageSize uint64,
) ([]RebaseInfo, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	found, err := s.findBaseDisk(ctx, tx, baseDiskID)
	if err != nil {
		return nil, err
	}

	if found == nil || found.isDoomed() {
		// Already retired.
		return nil, tx.Commit(ctx)
	}

	slots, err := s.getAcquiredSlots(ctx, tx, baseDiskID)
	if err != nil {
		return nil, err
	}

	logging.Info(
		ctx,
		"retireBaseDisk: baseDiskID=%v, slots=%+v",
		baseDiskID,
		slots,
	)

	var slotTransitions []slotTransition

	for i := 0; i < len(slots); i++ {
		oldState := slots[i]
		slotTransitions = append(slotTransitions, slotTransition{
			oldState: &oldState,
			state:    &slots[i],
		})
	}

	imageID := found.imageID
	zoneID := found.zoneID
	imageSize := found.imageSize
	if useImageSize != 0 {
		imageSize = useImageSize
	}

	config, err := s.getPoolConfig(ctx, session, imageID, zoneID)
	if err != nil {
		return nil, err
	}

	var suitableBaseDisks []baseDisk
	if config != nil {
		imageSize = config.imageSize

		suitableBaseDisks, err = s.getFreeBaseDisks(ctx, tx, imageID, zoneID)
		if err != nil {
			return nil, err
		}
	} else {
		// If pool is deleted then all non retiring base disks are suitable for
		// rebasing.
		suitableBaseDisks, err = s.getNonRetiringBaseDisks(ctx, tx, imageID, zoneID)
		if err != nil {
			return nil, err
		}
	}

	var baseDiskTransitions []baseDiskTransition

	for i := 0; i < len(suitableBaseDisks); i++ {
		state := &suitableBaseDisks[i]

		if state.id == baseDiskID || state.retiring {
			// Disk is not suitable for rebasing.
			continue
		}

		if len(state.srcDiskID) == 0 && state.status != baseDiskStatusReady {
			// Disk is not suitable for rebasing.
			continue
		}

		oldState := *state
		baseDiskTransitions = append(baseDiskTransitions, baseDiskTransition{
			oldState: &oldState,
			state:    state,
		})
	}

	var rebaseInfos []RebaseInfo
	slotIndex := 0
	baseDiskIndex := 0

	for slotIndex < len(slotTransitions) {
		slot := slotTransitions[slotIndex].state
		if len(slot.targetZoneID) != 0 {
			// Skip because relocate is in progress for this slot.
			slotIndex++
			continue
		}

		if len(slot.targetBaseDiskID) != 0 {
			// Should be idempotent.
			rebaseInfos = append(rebaseInfos, RebaseInfo{
				OverlayDisk: &types.Disk{
					ZoneId: slot.zoneID,
					DiskId: slot.overlayDiskID,
				},
				BaseDiskID:       slot.baseDiskID,
				TargetZoneID:     slot.targetZoneID,
				TargetBaseDiskID: slot.targetBaseDiskID,
				SlotGeneration:   slot.generation,
			})

			slotIndex++
			continue
		}

		if baseDiskIndex >= len(baseDiskTransitions) {
			baseDisk := s.generateBaseDisk(
				imageID,
				zoneID,
				imageSize,
				srcDisk,
			)
			logging.Info(
				ctx,
				"generated base disk: %+v",
				baseDisk,
			)

			baseDiskTransitions = append(baseDiskTransitions, baseDiskTransition{
				oldState: nil,
				state:    &baseDisk,
			})
			baseDiskIndex = len(baseDiskTransitions) - 1
		}

		target := baseDiskTransitions[baseDiskIndex].state

		if !target.hasFreeSlots() {
			baseDiskIndex++
			continue
		}

		err := acquireTargetUnitsAndSlots(ctx, tx, target, slot)
		if err != nil {
			return nil, err
		}

		slot.targetBaseDiskID = target.id
		slot.generation += 1

		rebaseInfos = append(rebaseInfos, RebaseInfo{
			OverlayDisk: &types.Disk{
				ZoneId: slot.zoneID,
				DiskId: slot.overlayDiskID,
			},
			BaseDiskID:       slot.baseDiskID,
			TargetBaseDiskID: slot.targetBaseDiskID,
			SlotGeneration:   slot.generation,
		})

		slotIndex++
	}

	updated := *found
	// Remove base disk from pool. It also removes base disk from 'free' table.
	updated.fromPool = false
	updated.retiring = true

	baseDiskTransitions = append(baseDiskTransitions, baseDiskTransition{
		oldState: found,
		state:    &updated,
	})

	err = s.updateBaseDisksAndSlots(ctx, tx, baseDiskTransitions, slotTransitions)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	logging.Info(
		ctx,
		"retired base disk %+v, rebaseInfos=%+v",
		updated,
		rebaseInfos,
	)
	return rebaseInfos, nil
}

func (s *storageYDB) isBaseDiskRetired(
	ctx context.Context,
	session *persistence.Session,
	baseDiskID string,
) (bool, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;

		select *
		from base_disks
		where id = $id
	`, s.tablesPath),
		persistence.ValueParam("$id", persistence.UTF8Value(baseDiskID)),
	)
	if err != nil {
		return false, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return true, nil
	}

	baseDisk, err := scanBaseDisk(res)
	if err != nil {
		return false, err
	}

	retired := baseDisk.isDoomed()
	if !retired {
		logging.Info(ctx, "base disk %+v is not retired", baseDisk)
	}

	return retired, nil
}

func (s *storageYDB) listBaseDisks(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
	zoneID string,
) ([]BaseDisk, error) {

	// TODO: should we limit scan here?
	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $zone_id as Utf8;
		declare $status as Int64;

		select *
		from base_disks
		where image_id = $image_id and zone_id = $zone_id and status = $status
	`, s.tablesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(zoneID)),
		persistence.ValueParam("$status", persistence.Int64Value(int64(baseDiskStatusReady))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var baseDisks []BaseDisk

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			baseDisk, err := scanBaseDisk(res)
			if err != nil {
				return nil, err
			}

			baseDisks = append(baseDisks, baseDisk.toBaseDisk())
		}
	}

	return baseDisks, nil
}

func (s *storageYDB) lockPool(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
	zoneID string,
	lockID string,
) (bool, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $zone_id as Utf8;

		select *
		from pools
		where image_id = $image_id and zone_id = $zone_id
	`, s.tablesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(zoneID)),
	)
	if err != nil {
		return false, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return false, commitErr
		}

		return false, errors.NewNonRetriableErrorf(
			"pool is not found, imageID=%v, zoneID=%v",
			imageID,
			zoneID,
		)
	}

	pool, err := scanPool(res)
	if err != nil {
		return false, err
	}

	if pool.status == poolStatusDeleted {
		err := tx.Commit(ctx)
		if err != nil {
			return false, err
		}

		return false, errors.NewNonRetriableErrorf(
			"pool is deleted, pool=%v",
			pool,
		)
	}

	if len(pool.lockID) != 0 {
		err = tx.Commit(ctx)
		if err != nil {
			return false, err
		}

		// Should be idempotent.
		return pool.lockID == lockID, nil
	}

	pool.lockID = lockID

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $pools as List<%v>;

		upsert into pools
		select *
		from AS_TABLE($pools)
	`, s.tablesPath, poolStructTypeString()),
		persistence.ValueParam("$pools", persistence.ListValue(pool.structValue())),
	)
	if err != nil {
		return false, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *storageYDB) unlockPool(
	ctx context.Context,
	session *persistence.Session,
	imageID string,
	zoneID string,
	lockID string,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $image_id as Utf8;
		declare $zone_id as Utf8;

		select *
		from pools
		where image_id = $image_id and zone_id = $zone_id
	`, s.tablesPath),
		persistence.ValueParam("$image_id", persistence.UTF8Value(imageID)),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(zoneID)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	pool, err := scanPool(res)
	if err != nil {
		return err
	}

	if pool.status == poolStatusDeleted {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	if len(pool.lockID) == 0 {
		// Should be idempotent.
		return tx.Commit(ctx)
	}

	if pool.lockID != lockID {
		err := tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"failed to unlock, another lock is in progress, lockID=%v",
			pool.lockID,
		)
	}

	pool.lockID = ""

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $pools as List<%v>;

		upsert into pools
		select *
		from AS_TABLE($pools)
	`, s.tablesPath, poolStructTypeString()),
		persistence.ValueParam("$pools", persistence.ListValue(pool.structValue())),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
