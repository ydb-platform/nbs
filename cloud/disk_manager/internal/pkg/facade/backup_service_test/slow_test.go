package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	dm "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
)

func TestBackupSlowSourceAndSnapshotWritesSSD(t *testing.T) {
	f := newFixture(t, dm.DiskKind_DISK_KIND_SSD)
	f.calibrate()
	require.Positive(t, f.readRate)
	require.Positive(t, f.snapshotRate)
	require.Positive(t, f.writeRate)
	for _, route := range []string{"nbs", "primary"} {
		source, id := f.id("slow-source"), f.id("slow-snapshot")
		f.empty(source, f.size)
		f.fill(source, 1)
		method := "ReadBlocks"
		control := f.readRate
		target := control / 10
		if route == "primary" {
			method = "PUT"
			control = f.snapshotRate
			target = control / 10
			if f.writeRate/4 < target {
				target = f.writeRate / 4
			}
		}
		rule := faultRule{ID: f.id("rate"), Route: route, Method: method, Mode: "rate", BytesPerSecond: int64(target)}
		require.Positive(t, rule.BytesPerSecond)
		f.rules(rule)
		op, err := f.createSnapshot(id, source)
		require.NoError(t, err)
		f.hit(rule.ID, f.window(f.createT0))
		injectEnd := time.Now().Add(maximum(60*time.Second, 3*f.period))
		var writeStart, writeEnd time.Time
		actualWriteRate := 0.0
		if route == "primary" {
			writeStart = time.Now()
			f.fill(source, 2)
			writeEnd = time.Now()
			actualWriteRate = float64(f.size) / writeEnd.Sub(writeStart).Seconds()
		}
		if wait := time.Until(injectEnd); wait > 0 {
			time.Sleep(wait)
		}
		measured, first, last := observedRate(f.events(), route, method, rule.ID, 0)
		require.Positive(t, measured, "a slow-mode test needs successful transfers, not only timeout errors")
		require.LessOrEqual(t, measured, control/10*1.02, "actual throughput must be at least 10 times lower")
		if route == "primary" {
			require.Less(t, measured, actualWriteRate/2)
			require.True(t, first.Before(writeEnd) && last.After(writeStart), "snapshot and disk writes must overlap")
		}
		t.Logf("RATE_EVIDENCE route=%s control=%.1f configured=%d measured=%.1f disk_write=%.1f first=%v last=%v writer=%v..%v", route, control, rule.BytesPerSecond, measured, actualWriteRate, first, last, writeStart, writeEnd)
		f.rules()
		_, err = f.wait(op, f.window(f.createT0))
		require.NoError(t, err)
		f.backupReady(backup.SnapshotMetaKey(source, id), id, 1, f.window(f.backupT0))
		// The immutable checkpoint oracle stays generation 1 even if the live
		// disk has already been overwritten with generation 2.
		f.deleteDisk(source)
		f.copies(id, source, 1, f.size)
	}
}
