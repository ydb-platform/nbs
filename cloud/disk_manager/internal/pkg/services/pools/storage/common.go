package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	pools_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func min(x, y uint64) uint64 {
	if x > y {
		return y
	}
	return x
}

func max(x, y uint64) uint64 {
	if y > x {
		return y
	}
	return x
}

func divideWithRoundingUp(x uint64, divisor uint64) uint64 {
	res := x / divisor

	if x%divisor != 0 {
		// Round up.
		return res + 1
	}

	return res
}

////////////////////////////////////////////////////////////////////////////////

func generateDiskID() string {
	return uuid.Must(uuid.NewV4()).String()
}

////////////////////////////////////////////////////////////////////////////////

type baseDiskStatus uint32

func (s *baseDiskStatus) UnmarshalYDB(res persistence.RawValue) error {
	*s = baseDiskStatus(res.Int64())
	return nil
}

// NOTE: These values are stored in DB, do not shuffle them around.
const (
	baseDiskStatusScheduling     baseDiskStatus = iota
	baseDiskStatusCreating       baseDiskStatus = iota
	baseDiskStatusReady          baseDiskStatus = iota
	baseDiskStatusDeleting       baseDiskStatus = iota
	baseDiskStatusDeleted        baseDiskStatus = iota
	baseDiskStatusCreationFailed baseDiskStatus = iota
)

func baseDiskStatusToString(status baseDiskStatus) string {
	switch status {
	case baseDiskStatusScheduling:
		return "scheduling"
	case baseDiskStatusCreating:
		return "creating"
	case baseDiskStatusReady:
		return "ready"
	case baseDiskStatusDeleting:
		return "deleting"
	case baseDiskStatusDeleted:
		return "deleted"
	case baseDiskStatusCreationFailed:
		return "creation_failed"
	}

	return fmt.Sprintf("unknown_%v", status)
}

////////////////////////////////////////////////////////////////////////////////

type baseDisk struct {
	id                  string
	imageID             string
	zoneID              string
	checkpointID        string
	createTaskID        string
	imageSize           uint64
	size                uint64
	srcDiskZoneID       string
	srcDiskID           string
	srcDiskCheckpointID string

	activeSlots    uint64
	maxActiveSlots uint64
	activeUnits    uint64
	units          uint64
	fromPool       bool
	retiring       bool
	deletedAt      time.Time
	status         baseDiskStatus
}

func (d *baseDisk) toBaseDisk() BaseDisk {
	var srcDisk *types.Disk
	if len(d.srcDiskID) != 0 {
		srcDisk = &types.Disk{
			ZoneId: d.srcDiskZoneID,
			DiskId: d.srcDiskID,
		}
	}

	return BaseDisk{
		ID:      d.id,
		ImageID: d.imageID,
		ZoneID:  d.zoneID,

		SrcDisk:             srcDisk,
		SrcDiskCheckpointID: d.srcDiskCheckpointID,
		CheckpointID:        d.checkpointID,
		CreateTaskID:        d.createTaskID,
		Size:                d.size,
		Ready:               d.status == baseDiskStatusReady,
	}
}

func (d *baseDisk) isInflight() bool {
	return d.status == baseDiskStatusScheduling || d.status == baseDiskStatusCreating
}

func (d *baseDisk) isDoomed() bool {
	return d.status >= baseDiskStatusDeleting
}

func (d *baseDisk) applyInvariants() {
	if d.activeUnits == 0 {
		if d.status == baseDiskStatusCreationFailed {
			d.status = baseDiskStatusDeleting
			return
		}

		// If base disk is not from pool and it does not have active slots then
		// it should be deleted.
		if !d.isDoomed() && !d.fromPool {
			d.status = baseDiskStatusDeleting
		}
	}
}

func (d *baseDisk) freeSlots() uint64 {
	if d.isDoomed() || !d.fromPool {
		return 0
	}

	if d.units != 0 && d.activeUnits >= d.units {
		return 0
	}

	if d.maxActiveSlots < d.activeSlots {
		return 0
	}

	return d.maxActiveSlots - d.activeSlots
}

func (d *baseDisk) freeUnits() uint64 {
	slots := d.freeSlots()

	if slots == 0 {
		return 0
	}

	return d.units - d.activeUnits
}

func (d *baseDisk) hasFreeSlots() bool {
	return d.freeSlots() != 0
}

func (d *baseDisk) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("id", persistence.UTF8Value(d.id)),
		persistence.StructFieldValue("image_id", persistence.UTF8Value(d.imageID)),
		persistence.StructFieldValue("zone_id", persistence.UTF8Value(d.zoneID)),
		persistence.StructFieldValue("src_disk_zone_id", persistence.UTF8Value(d.srcDiskZoneID)),
		persistence.StructFieldValue("src_disk_id", persistence.UTF8Value(d.srcDiskID)),
		persistence.StructFieldValue("src_disk_checkpoint_id", persistence.UTF8Value(d.srcDiskCheckpointID)),
		persistence.StructFieldValue("checkpoint_id", persistence.UTF8Value(d.checkpointID)),
		persistence.StructFieldValue("create_task_id", persistence.UTF8Value(d.createTaskID)),
		persistence.StructFieldValue("image_size", persistence.Uint64Value(d.imageSize)),
		persistence.StructFieldValue("size", persistence.Uint64Value(d.size)),

		persistence.StructFieldValue("active_slots", persistence.Uint64Value(d.activeSlots)),
		persistence.StructFieldValue("max_active_slots", persistence.Uint64Value(d.maxActiveSlots)),
		persistence.StructFieldValue("active_units", persistence.Uint64Value(d.activeUnits)),
		persistence.StructFieldValue("units", persistence.Uint64Value(d.units)),
		persistence.StructFieldValue("from_pool", persistence.BoolValue(d.fromPool)),
		persistence.StructFieldValue("retiring", persistence.BoolValue(d.retiring)),
		persistence.StructFieldValue("deleted_at", persistence.TimestampValue(d.deletedAt)),
		persistence.StructFieldValue("status", persistence.Int64Value(int64(d.status))),
	)
}

func baseDiskStructTypeString() string {
	return `Struct<
		id: Utf8,
		image_id: Utf8,
		zone_id: Utf8,
		src_disk_zone_id: Utf8,
		src_disk_id: Utf8,
		src_disk_checkpoint_id: Utf8,
		checkpoint_id: Utf8,
		create_task_id: Utf8,
		image_size: Uint64,
		size: Uint64,

		active_slots: Uint64,
		max_active_slots: Uint64,
		active_units: Uint64,
		units: Uint64,
		from_pool: Bool,
		retiring: Bool,
		deleted_at: Timestamp,
		status: Int64>`
}

func baseDisksTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("image_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("src_disk_zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("src_disk_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("src_disk_checkpoint_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("checkpoint_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("create_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("image_size", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("size", persistence.Optional(persistence.TypeUint64)),

		persistence.WithColumn("active_slots", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("max_active_slots", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("active_units", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("units", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("from_pool", persistence.Optional(persistence.TypeBool)),
		persistence.WithColumn("retiring", persistence.Optional(persistence.TypeBool)),
		persistence.WithColumn("deleted_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("status", persistence.Optional(persistence.TypeInt64)),

		persistence.WithPrimaryKeyColumn("id"),
	)
}

func scanBaseDisk(res persistence.Result) (baseDisk baseDisk, err error) {
	err = res.ScanNamed(
		persistence.OptionalWithDefault("id", &baseDisk.id),
		persistence.OptionalWithDefault("image_id", &baseDisk.imageID),
		persistence.OptionalWithDefault("zone_id", &baseDisk.zoneID),
		persistence.OptionalWithDefault("src_disk_zone_id", &baseDisk.srcDiskZoneID),
		persistence.OptionalWithDefault("src_disk_id", &baseDisk.srcDiskID),
		persistence.OptionalWithDefault("src_disk_checkpoint_id", &baseDisk.srcDiskCheckpointID),
		persistence.OptionalWithDefault("checkpoint_id", &baseDisk.checkpointID),
		persistence.OptionalWithDefault("create_task_id", &baseDisk.createTaskID),
		persistence.OptionalWithDefault("image_size", &baseDisk.imageSize),
		persistence.OptionalWithDefault("size", &baseDisk.size),

		persistence.OptionalWithDefault("active_slots", &baseDisk.activeSlots),
		persistence.OptionalWithDefault("max_active_slots", &baseDisk.maxActiveSlots),
		persistence.OptionalWithDefault("active_units", &baseDisk.activeUnits),
		persistence.OptionalWithDefault("units", &baseDisk.units),
		persistence.OptionalWithDefault("from_pool", &baseDisk.fromPool),
		persistence.OptionalWithDefault("retiring", &baseDisk.retiring),
		persistence.OptionalWithDefault("deleted_at", &baseDisk.deletedAt),
		persistence.OptionalWithDefault("status", &baseDisk.status),
	)
	return
}

func scanBaseDisks(
	ctx context.Context,
	res persistence.Result,
) ([]baseDisk, error) {

	var baseDisks []baseDisk
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			baseDisk, err := scanBaseDisk(res)
			if err != nil {
				return nil, err
			}

			baseDisks = append(baseDisks, baseDisk)
		}
	}

	return baseDisks, nil
}

////////////////////////////////////////////////////////////////////////////////

type baseDiskTransition struct {
	oldState *baseDisk
	state    *baseDisk
}

type slotTransition struct {
	oldState *slot
	state    *slot
}

type poolTransition struct {
	oldState pool
	state    pool
}

type poolAction struct {
	sizeDiff              int64
	freeUnitsDiff         int64
	acquiredUnitsDiff     int64
	baseDisksInflightDiff int64
}

func (a *poolAction) hasChanges() bool {
	return a.sizeDiff != 0 ||
		a.freeUnitsDiff != 0 ||
		a.acquiredUnitsDiff != 0 ||
		a.baseDisksInflightDiff != 0
}

func (a *poolAction) apply(state *pool) {
	state.size = uint64(int64(state.size) + a.sizeDiff)
	state.freeUnits = uint64(int64(state.freeUnits) + a.freeUnitsDiff)
	state.acquiredUnits = uint64(int64(state.acquiredUnits) + a.acquiredUnitsDiff)
	state.baseDisksInflight = uint64(
		int64(state.baseDisksInflight) + a.baseDisksInflightDiff,
	)
}

func computePoolAction(t baseDiskTransition) (poolAction, error) {
	var a poolAction

	if t.oldState == nil {
		if t.state.fromPool {
			// Add all free slots to pool when creating new base disk.
			a.sizeDiff = int64(t.state.freeSlots())
			a.freeUnitsDiff = int64(t.state.freeUnits())
			a.acquiredUnitsDiff = int64(t.state.activeUnits)

			if t.state.isInflight() {
				a.baseDisksInflightDiff = 1
			}
		}

		return a, nil
	}

	if !t.oldState.fromPool {
		// Returning base disk to pool is forbidden (otherwise it's nothing to
		// do here).
		return a, nil
	}

	switch {
	case !t.oldState.isDoomed() && t.state.isDoomed():
		a.sizeDiff = -int64(t.oldState.freeSlots())
		a.freeUnitsDiff = -int64(t.oldState.freeUnits())
		a.acquiredUnitsDiff = -int64(t.oldState.activeUnits)

		if t.state.activeUnits != 0 {
			return poolAction{}, errors.NewNonRetriableErrorf(
				"internal inconsistency: invalid base disk state %+v",
				t.state,
			)
		}

	case !t.state.fromPool:
		// Remove all free slots from pool when ejecting base disk.
		a.sizeDiff = -int64(t.oldState.freeSlots())
		a.freeUnitsDiff = -int64(t.oldState.freeUnits())
		a.acquiredUnitsDiff = -int64(t.oldState.activeUnits)

		if t.oldState.activeUnits != t.state.activeUnits {
			return poolAction{}, errors.NewNonRetriableErrorf(
				"internal inconsistency: invalid base disk transition from %+v to %+v",
				t.oldState,
				t.state,
			)
		}

	case !t.state.isDoomed():
		// Regular transition for base disks.
		a.sizeDiff = int64(t.state.freeSlots()) - int64(t.oldState.freeSlots())
		a.freeUnitsDiff = int64(t.state.freeUnits()) - int64(t.oldState.freeUnits())
		a.acquiredUnitsDiff = int64(t.state.activeUnits) - int64(t.oldState.activeUnits)
	}

	if t.oldState.isInflight() && !t.state.isInflight() {
		a.baseDisksInflightDiff = -1
	}

	// NOTE: already existing base disk can't switch its state from
	// 'not inflight' to 'inflight'.

	return a, nil
}

////////////////////////////////////////////////////////////////////////////////

type slotStatus uint32

func (s *slotStatus) UnmarshalYDB(res persistence.RawValue) error {
	*s = slotStatus(res.Int64())
	return nil
}

// NOTE: These values are stored in DB, do not shuffle them around.
const (
	slotStatusAcquired slotStatus = iota
	slotStatusReleased slotStatus = iota
)

func slotStatusToString(status slotStatus) string {
	switch status {
	case slotStatusAcquired:
		return "acquired"
	case slotStatusReleased:
		return "released"
	}

	return fmt.Sprintf("unknown_%v", status)
}

////////////////////////////////////////////////////////////////////////////////

type slot struct {
	overlayDiskID       string
	overlayDiskKind     types.DiskKind
	overlayDiskSize     uint64
	baseDiskID          string
	imageID             string
	zoneID              string
	allottedSlots       uint64
	allottedUnits       uint64
	releasedAt          time.Time
	targetZoneID        string
	targetBaseDiskID    string
	targetAllottedSlots uint64
	targetAllottedUnits uint64
	generation          uint64
	status              slotStatus
}

func (s *slot) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("overlay_disk_id", persistence.UTF8Value(s.overlayDiskID)),
		persistence.StructFieldValue("overlay_disk_kind", persistence.Int64Value(int64(s.overlayDiskKind))),
		persistence.StructFieldValue("overlay_disk_size", persistence.Uint64Value(s.overlayDiskSize)),
		persistence.StructFieldValue("base_disk_id", persistence.UTF8Value(s.baseDiskID)),
		persistence.StructFieldValue("image_id", persistence.UTF8Value(s.imageID)),
		persistence.StructFieldValue("zone_id", persistence.UTF8Value(s.zoneID)),
		persistence.StructFieldValue("allotted_slots", persistence.Uint64Value(s.allottedSlots)),
		persistence.StructFieldValue("allotted_units", persistence.Uint64Value(s.allottedUnits)),
		persistence.StructFieldValue("released_at", persistence.TimestampValue(s.releasedAt)),
		persistence.StructFieldValue("target_zone_id", persistence.UTF8Value(s.targetZoneID)),
		persistence.StructFieldValue("target_base_disk_id", persistence.UTF8Value(s.targetBaseDiskID)),
		persistence.StructFieldValue("target_allotted_slots", persistence.Uint64Value(s.targetAllottedSlots)),
		persistence.StructFieldValue("target_allotted_units", persistence.Uint64Value(s.targetAllottedUnits)),
		persistence.StructFieldValue("generation", persistence.Uint64Value(s.generation)),
		persistence.StructFieldValue("status", persistence.Int64Value(int64(s.status))),
	)
}

func slotStructTypeString() string {
	return `Struct<
		overlay_disk_id: Utf8,
		overlay_disk_kind: Int64,
		overlay_disk_size: Uint64,
		base_disk_id: Utf8,
		image_id: Utf8,
		zone_id: Utf8,
		allotted_slots: Uint64,
		allotted_units: Uint64,
		released_at: Timestamp,
		target_zone_id: Utf8,
		target_base_disk_id: Utf8,
		target_allotted_slots: Uint64,
		target_allotted_units: Uint64,
		generation: Uint64,
		status: Int64>`
}

func slotsTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("overlay_disk_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("overlay_disk_kind", persistence.Optional(persistence.TypeInt64)),
		persistence.WithColumn("overlay_disk_size", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("base_disk_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("image_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("status", persistence.Optional(persistence.TypeInt64)),
		persistence.WithColumn("allotted_slots", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("allotted_units", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("released_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("target_zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("target_base_disk_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("target_allotted_slots", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("target_allotted_units", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("generation", persistence.Optional(persistence.TypeUint64)),
		persistence.WithPrimaryKeyColumn("overlay_disk_id"),
	)
}

func scanSlot(res persistence.Result) (slot slot, err error) {
	var diskKind int64
	err = res.ScanNamed(
		persistence.OptionalWithDefault("overlay_disk_id", &slot.overlayDiskID),
		persistence.OptionalWithDefault("overlay_disk_kind", &diskKind),
		persistence.OptionalWithDefault("overlay_disk_size", &slot.overlayDiskSize),
		persistence.OptionalWithDefault("base_disk_id", &slot.baseDiskID),
		persistence.OptionalWithDefault("image_id", &slot.imageID),
		persistence.OptionalWithDefault("zone_id", &slot.zoneID),
		persistence.OptionalWithDefault("allotted_slots", &slot.allottedSlots),
		persistence.OptionalWithDefault("allotted_units", &slot.allottedUnits),
		persistence.OptionalWithDefault("released_at", &slot.releasedAt),
		persistence.OptionalWithDefault("target_zone_id", &slot.targetZoneID),
		persistence.OptionalWithDefault("target_base_disk_id", &slot.targetBaseDiskID),
		persistence.OptionalWithDefault("target_allotted_slots", &slot.targetAllottedSlots),
		persistence.OptionalWithDefault("target_allotted_units", &slot.targetAllottedUnits),
		persistence.OptionalWithDefault("generation", &slot.generation),
		persistence.OptionalWithDefault("status", &slot.status),
	)
	if err != nil {
		return
	}

	slot.overlayDiskKind = types.DiskKind(diskKind)
	return
}

////////////////////////////////////////////////////////////////////////////////

type poolConfig struct {
	imageID   string
	zoneID    string
	capacity  uint64
	imageSize uint64
}

func scanPoolConfig(res persistence.Result) (config poolConfig, err error) {
	err = res.ScanNamed(
		persistence.OptionalWithDefault("image_id", &config.imageID),
		persistence.OptionalWithDefault("zone_id", &config.zoneID),
		persistence.OptionalWithDefault("capacity", &config.capacity),
		persistence.OptionalWithDefault("image_size", &config.imageSize),
	)
	return
}

////////////////////////////////////////////////////////////////////////////////

type poolStatus uint32

func (s *poolStatus) UnmarshalYDB(res persistence.RawValue) error {
	*s = poolStatus(res.Int64())
	return nil
}

// NOTE: These values are stored in DB, do not shuffle them around.
const (
	poolStatusReady   poolStatus = iota
	poolStatusDeleted poolStatus = iota
)

func poolStatusToString(status poolStatus) string {
	switch status {
	case poolStatusReady:
		return "ready"
	case poolStatusDeleted:
		return "deleted"
	}

	return fmt.Sprintf("unknown_%v", status)
}

type pool struct {
	imageID           string
	zoneID            string
	size              uint64
	freeUnits         uint64
	acquiredUnits     uint64
	baseDisksInflight uint64
	lockID            string
	status            poolStatus
	createdAt         time.Time
}

func (p *pool) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("image_id", persistence.UTF8Value(p.imageID)),
		persistence.StructFieldValue("zone_id", persistence.UTF8Value(p.zoneID)),
		persistence.StructFieldValue("size", persistence.Uint64Value(p.size)),
		persistence.StructFieldValue("free_units", persistence.Uint64Value(p.freeUnits)),
		persistence.StructFieldValue("acquired_units", persistence.Uint64Value(p.acquiredUnits)),
		persistence.StructFieldValue("base_disks_inflight", persistence.Uint64Value(p.baseDisksInflight)),
		persistence.StructFieldValue("lock_id", persistence.UTF8Value(p.lockID)),
		persistence.StructFieldValue("status", persistence.Int64Value(int64(p.status))),
		persistence.StructFieldValue("created_at", persistence.TimestampValue(p.createdAt)),
	)
}

func poolStructTypeString() string {
	return `Struct<
		image_id: Utf8,
		zone_id: Utf8,
		size: Uint64,
		acquired_units: Uint64,
		free_units: Uint64,
		base_disks_inflight: Uint64,
		lock_id: Utf8,
		status: Int64,
		created_at: Timestamp>`
}

func poolsTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("image_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("size", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("free_units", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("acquired_units", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("base_disks_inflight", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("lock_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("status", persistence.Optional(persistence.TypeInt64)),
		persistence.WithColumn("created_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithPrimaryKeyColumn("image_id", "zone_id"),
	)
}

func scanPool(res persistence.Result) (pool pool, err error) {
	err = res.ScanNamed(
		persistence.OptionalWithDefault("image_id", &pool.imageID),
		persistence.OptionalWithDefault("zone_id", &pool.zoneID),
		persistence.OptionalWithDefault("size", &pool.size),
		persistence.OptionalWithDefault("free_units", &pool.freeUnits),
		persistence.OptionalWithDefault("acquired_units", &pool.acquiredUnits),
		persistence.OptionalWithDefault("base_disks_inflight", &pool.baseDisksInflight),
		persistence.OptionalWithDefault("lock_id", &pool.lockID),
		persistence.OptionalWithDefault("status", &pool.status),
		persistence.OptionalWithDefault("created_at", &pool.createdAt),
	)
	if err != nil {
		return
	}

	if pool.createdAt.IsZero() {
		pool.createdAt = time.Now()
	}

	return
}

////////////////////////////////////////////////////////////////////////////////

// TODO: move some constants to configs.
const (
	// 32 GB.
	baseDiskUnitSize            = uint64(32 << 30)
	minBaseDiskUnits            = 30
	baseDiskOverSubscription    = 2
	overlayDiskOversubscription = 30
	// SSD units should be more valuable than HDD.
	ssdUnitMultiplier = 5
)

func (s *storageYDB) generateBaseDisk(
	imageID string,
	zoneID string,
	imageSize uint64,
	srcDisk *types.Disk,
) baseDisk {

	var size, maxActiveSlots, units uint64

	if imageSize == 0 {
		// Default case.
		units = s.maxBaseDiskUnits
		maxActiveSlots = s.maxActiveSlots
	} else {
		// Base disks are using SSD.
		ssdUnits := divideWithRoundingUp(
			imageSize,
			baseDiskUnitSize,
		)
		size = ssdUnits * baseDiskUnitSize

		units = ssdUnitMultiplier * ssdUnits
		units = baseDiskOverSubscription * units
		units = max(units, minBaseDiskUnits)
		units = min(units, s.maxBaseDiskUnits)

		maxActiveSlots = min(units, s.maxActiveSlots)
	}

	var srcDiskZoneID string
	var srcDiskID string
	if srcDisk != nil {
		srcDiskZoneID = srcDisk.ZoneId
		srcDiskID = srcDisk.DiskId
	}

	return baseDisk{
		id:                  generateDiskID(),
		imageID:             imageID,
		zoneID:              zoneID,
		checkpointID:        imageID, // Note: we use image id as checkpoint id.
		createTaskID:        "",      // Will be determined later.
		imageSize:           imageSize,
		size:                size,
		srcDiskZoneID:       srcDiskZoneID,
		srcDiskID:           srcDiskID,
		srcDiskCheckpointID: imageID, // Note: we use image id as checkpoint id.

		maxActiveSlots: maxActiveSlots,
		units:          units,
		fromPool:       true,
		status:         baseDiskStatusScheduling,
	}
}

func computeAllottedUnits(s *slot) uint64 {
	var res uint64

	overlayDiskUnits := s.overlayDiskSize / baseDiskUnitSize

	switch s.overlayDiskKind {
	case types.DiskKind_DISK_KIND_SSD:
		res = divideWithRoundingUp(
			ssdUnitMultiplier*overlayDiskUnits,
			overlayDiskOversubscription,
		)
	case types.DiskKind_DISK_KIND_HDD:
		res = divideWithRoundingUp(
			overlayDiskUnits,
			overlayDiskOversubscription,
		)
	}

	// Can't allocate less than 1 unit.
	return max(1, res)
}

////////////////////////////////////////////////////////////////////////////////

func acquireUnitsAndSlots(
	ctx context.Context,
	tx *persistence.Transaction,
	disk *baseDisk,
	slot *slot,
) error {

	if !disk.hasFreeSlots() {
		err := tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"acquireUnitsAndSlots failed: base disk has no free slots, disk %+v, slot %+v",
			disk,
			slot,
		)
	}

	slot.allottedSlots = 1
	disk.activeSlots++

	slot.allottedUnits = computeAllottedUnits(slot)
	disk.activeUnits += slot.allottedUnits

	return nil
}

func acquireTargetUnitsAndSlots(
	ctx context.Context,
	tx *persistence.Transaction,
	disk *baseDisk,
	slot *slot,
) error {

	if !disk.hasFreeSlots() {
		err := tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"acquireTargetUnitsAndSlots failed: base disk has no free slots, disk %+v, slot %+v",
			disk,
			slot,
		)
	}

	slot.targetAllottedSlots = 1
	disk.activeSlots++

	slot.targetAllottedUnits = computeAllottedUnits(slot)
	disk.activeUnits += slot.targetAllottedUnits

	return nil
}

func releaseUnitsAndSlots(
	ctx context.Context,
	tx *persistence.Transaction,
	disk *baseDisk,
	slot slot,
) error {

	if slot.allottedSlots == 0 {
		if disk.activeSlots == 0 {
			err := tx.Commit(ctx)
			if err != nil {
				return err
			}

			return errors.NewNonRetriableErrorf(
				"internal inconsistency: disk %+v activeSlots should be greater than zero",
				disk,
			)
		}

		disk.activeSlots--
	} else {
		if disk.activeSlots < slot.allottedSlots {
			err := tx.Commit(ctx)
			if err != nil {
				return err
			}

			return errors.NewNonRetriableErrorf(
				"internal inconsistency: disk %+v activeSlots should be greater than allottedSlots %v",
				disk,
				slot.allottedSlots,
			)
		}

		disk.activeSlots -= slot.allottedSlots
	}

	if disk.activeUnits < slot.allottedUnits {
		err := tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"internal inconsistency: disk %+v activeUnits should be greater than allottedUnits %v",
			disk,
			slot.allottedUnits,
		)
	}
	disk.activeUnits -= slot.allottedUnits

	return nil
}

func releaseTargetUnitsAndSlots(
	ctx context.Context,
	tx *persistence.Transaction,
	disk *baseDisk,
	slot slot,
) error {

	if slot.targetAllottedSlots == 0 {
		if disk.activeSlots == 0 {
			err := tx.Commit(ctx)
			if err != nil {
				return err
			}

			return errors.NewNonRetriableErrorf(
				"internal inconsistency: disk %+v activeSlots should be greater than zero",
				disk,
			)
		}

		disk.activeSlots--
	} else {
		if disk.activeSlots < slot.targetAllottedSlots {
			err := tx.Commit(ctx)
			if err != nil {
				return err
			}

			return errors.NewNonRetriableErrorf(
				"internal inconsistency: disk %+v activeSlots should be greater than targetAllottedSlots %v",
				disk,
				slot.targetAllottedSlots,
			)
		}

		disk.activeSlots -= slot.targetAllottedSlots
	}

	if disk.activeUnits < slot.targetAllottedUnits {
		err := tx.Commit(ctx)
		if err != nil {
			return err
		}

		return errors.NewNonRetriableErrorf(
			"internal inconsistency: disk %+v activeUnits should be greater than targetAllottedUnits %v",
			disk,
			slot.targetAllottedUnits,
		)
	}
	disk.activeUnits -= slot.targetAllottedUnits

	return nil
}

////////////////////////////////////////////////////////////////////////////////

type baseDiskKey struct {
	imageID    string
	zoneID     string
	baseDiskID string
}

func (k *baseDiskKey) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("image_id", persistence.UTF8Value(k.imageID)),
		persistence.StructFieldValue("zone_id", persistence.UTF8Value(k.zoneID)),
		persistence.StructFieldValue("base_disk_id", persistence.UTF8Value(k.baseDiskID)),
	)
}

func baseDiskKeyStructTypeString() string {
	return `Struct<
		image_id: Utf8,
		zone_id: Utf8,
		base_disk_id: Utf8>`
}

func keyFromBaseDisk(baseDisk *baseDisk) baseDiskKey {
	return baseDiskKey{
		imageID:    baseDisk.imageID,
		zoneID:     baseDisk.zoneID,
		baseDiskID: baseDisk.id,
	}
}

////////////////////////////////////////////////////////////////////////////////

func CreateYDBTables(
	ctx context.Context,
	config *pools_config.PoolsConfig,
	db *persistence.YDBClient,
	dropUnusedColumns bool,
) error {

	logging.Info(ctx, "Creating tables for pools in %v", db.AbsolutePath(config.GetStorageFolder()))

	err := db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"base_disks",
		baseDisksTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created base_disks table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"slots",
		slotsTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created slots table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"overlay_disk_ids",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("base_disk_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("overlay_disk_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("base_disk_id", "overlay_disk_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created overlay_disk_ids table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"configs",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("image_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("kind", persistence.Optional(persistence.TypeInt64)), // deprecated
			persistence.WithColumn("capacity", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("image_size", persistence.Optional(persistence.TypeUint64)),
			persistence.WithPrimaryKeyColumn("image_id", "zone_id", "kind"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created configs table")

	// Contains ids of base disks that currently have status "scheduling".
	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"scheduling",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("image_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("base_disk_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("image_id", "zone_id", "base_disk_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created scheduling table")

	// Contains ids of base disks that currently have free (not acquired) slots.
	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"free",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("image_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("base_disk_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("image_id", "zone_id", "base_disk_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created free table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"pools",
		poolsTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created pools table")

	// Contains ids of base disks that currently have status "deleting".
	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"deleting",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("base_disk_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("base_disk_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created deleting table")

	// Contains ids base disks that currently have status "deleted".
	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"deleted",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("deleted_at", persistence.Optional(persistence.TypeTimestamp)),
			persistence.WithColumn("base_disk_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("deleted_at", "base_disk_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created deleted table")

	// Contains ids of overlay disks that currently have status "released".
	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"released",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("released_at", persistence.Optional(persistence.TypeTimestamp)),
			persistence.WithColumn("overlay_disk_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("released_at", "overlay_disk_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created released table")

	logging.Info(ctx, "Created tables for pools")

	return nil
}

func DropYDBTables(
	ctx context.Context,
	config *pools_config.PoolsConfig,
	db *persistence.YDBClient,
) error {

	logging.Info(ctx, "Dropping tables for pools in %v", db.AbsolutePath(config.GetStorageFolder()))

	err := db.DropTable(ctx, config.GetStorageFolder(), "base_disks")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped base_disks table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "slots")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped slots table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "overlay_disk_ids")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped overlay_disk_ids table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "configs")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped configs table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "scheduling")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped scheduling table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "free")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped free table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "pools")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped pools table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "deleting")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped deleting table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "deleted")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped deleted table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "released")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped released table")

	logging.Info(ctx, "Dropped tables for pools")

	return nil
}
