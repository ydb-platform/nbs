package tests

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	dm "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
)

func (f *fixture) restoreImage(image string, generation int) string {
	original := f.ctx
	ctx, cancel := context.WithTimeout(original, f.window(f.restoreT0))
	f.ctx = ctx
	defer func() { f.ctx = original; cancel() }()
	id := f.id("from-image")
	op, err := f.dm.CreateDisk(f.req(), &dm.CreateDiskRequest{Src: &dm.CreateDiskRequest_SrcImageId{SrcImageId: image}, DiskId: diskID(id), Kind: f.kind, Size: int64(f.size), BlockSize: blockSize})
	f.success(op, err, f.window(f.restoreT0))
	f.verifyDisk(id, f.size, generation)
	return id
}
func (f *fixture) imageCopies(id string, generation int) {
	f.deleteDisk(f.restoreImage(id, generation))
	f.rules(faultRule{ID: f.id("primary-off"), Route: "primary", Mode: "error"})
	reader := f.backupReady(backup.ImageMetaKey(id), id, generation, f.window(f.backupT0))
	f.deleteDisk(f.restoreBackup(reader, generation))
	f.rules()
}
func TestBackupImagesAllPublicSources(t *testing.T) {
	f := newFixture(t, dm.DiskKind_DISK_KIND_SSD)
	source, snapshot := f.id("source"), f.id("snapshot")
	f.empty(source, f.size)
	f.fill(source, 1)
	op, err := f.createSnapshot(snapshot, source)
	f.success(op, err, 5*time.Minute)
	snapshotReader := f.backupReady(backup.SnapshotMetaKey(source, snapshot), snapshot, 1, 5*time.Minute)
	raw := make([]byte, 0, f.size)
	for i := 0; uint64(i*chunkSize) < f.size; i++ {
		raw = append(raw, pattern(1, i)...)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"fixed-independent-oracle"`)
		http.ServeContent(w, r, "oracle.raw", time.Unix(1000, 0), bytes.NewReader(raw))
	}))
	defer server.Close()
	diskImage, snapshotImage, childImage, urlImage := f.id("disk-image"), f.id("snapshot-image"), f.id("child-image"), f.id("url-image")
	requests := []*dm.CreateImageRequest{
		{Src: &dm.CreateImageRequest_SrcDiskId{SrcDiskId: diskID(source)}, DstImageId: diskImage},
		{Src: &dm.CreateImageRequest_SrcSnapshotId{SrcSnapshotId: snapshot}, DstImageId: snapshotImage},
		{Src: &dm.CreateImageRequest_SrcImageId{SrcImageId: snapshotImage}, DstImageId: childImage},
		{Src: &dm.CreateImageRequest_SrcUrl{SrcUrl: &dm.ImageUrl{Url: server.URL + "/oracle.raw"}}, DstImageId: urlImage},
	}
	for _, req := range requests {
		req.FolderId = "backup-acceptance"
		op, err = f.dm.CreateImage(f.req(), req)
		f.success(op, err, 5*time.Minute)
		reader := f.backupReady(backup.ImageMetaKey(req.DstImageId), req.DstImageId, 1, 5*time.Minute)
		require.Equal(t, f.size, reader.size)
		if req.DstImageId == snapshotImage || req.DstImageId == childImage {
			require.Equal(t, snapshotReader.ids, reader.ids, "shallow image copies must preserve inherited chunk references")
		}
	}
	f.deleteDisk(source)
	op, err = f.dm.DeleteSnapshot(f.req(), &dm.DeleteSnapshotRequest{SnapshotId: snapshot})
	f.success(op, err, 5*time.Minute)
	for _, req := range requests {
		f.imageCopies(req.DstImageId, 1)
	}
	// Deleting the parent image must not erase a descendant's inherited data.
	op, err = f.dm.DeleteImage(f.req(), &dm.DeleteImageRequest{ImageId: snapshotImage})
	f.success(op, err, 5*time.Minute)
	f.imageCopies(childImage, 1)
	for _, id := range []string{diskImage, childImage, urlImage} {
		for attempt := 0; attempt < 2; attempt++ {
			op, err = f.dm.DeleteImage(f.req(), &dm.DeleteImageRequest{ImageId: id})
			f.success(op, err, 5*time.Minute)
		}
	}
}
func TestBackupInvalidImageAndDiskSources(t *testing.T) {
	f := newFixture(t, dm.DiskKind_DISK_KIND_SSD)
	for _, req := range []*dm.CreateImageRequest{
		{Src: &dm.CreateImageRequest_SrcDiskId{SrcDiskId: diskID("absent")}},
		{Src: &dm.CreateImageRequest_SrcSnapshotId{SrcSnapshotId: "absent"}},
		{Src: &dm.CreateImageRequest_SrcImageId{SrcImageId: "absent"}},
	} {
		req.DstImageId = f.id("invalid-image")
		req.FolderId = "backup-acceptance"
		op, err := f.dm.CreateImage(f.req(), req)
		f.failed(op, err, 5*time.Minute)
		storage, closeStorage := testcommon.NewResourceStorage(t, f.ctx)
		meta, metaErr := storage.GetImageMeta(f.ctx, req.DstImageId)
		closeStorage()
		require.NoError(t, metaErr)
		if meta != nil {
			require.False(t, meta.Ready)
		}
		_, err = openBackup(f.ctx, f.backup, backup.ImageMetaKey(req.DstImageId), req.DstImageId)
		require.Error(t, err)
	}
	for _, req := range []*dm.CreateDiskRequest{
		{Src: &dm.CreateDiskRequest_SrcSnapshotId{SrcSnapshotId: "absent"}},
		{Src: &dm.CreateDiskRequest_SrcImageId{SrcImageId: "absent"}},
	} {
		req.DiskId = diskID(f.id("invalid-disk"))
		req.Kind = f.kind
		req.Size = int64(f.size)
		req.BlockSize = blockSize
		op, err := f.dm.CreateDisk(f.req(), req)
		f.failed(op, err, 5*time.Minute)
	}
}
func TestBackupFollowerFailureIsolatedFromIndependentSnapshot(t *testing.T) {
	f := newFixture(t, dm.DiskKind_DISK_KIND_SSD)
	f.calibrate()
	badSource, bad := f.id("bad-source"), f.id("bad-snapshot")
	goodSource, good := f.id("good-source"), f.id("good-snapshot")
	f.empty(badSource, f.size)
	f.fill(badSource, 1)
	f.empty(goodSource, f.size)
	f.fill(goodSource, 2)
	rule := faultRule{ID: f.id("bad-copy"), Route: "backup", Method: "PUT", Contains: "." + bad + ".", Mode: "error"}
	f.rules(rule)
	op, err := f.createSnapshot(bad, badSource)
	f.success(op, err, f.window(f.createT0))
	f.hit(rule.ID, f.window(f.backupT0))
	// Both operations overlap the same injected outage, but only one copy fails.
	healthy, err := f.createSnapshot(good, goodSource)
	f.success(healthy, err, f.window(f.createT0))
	f.backupReady(backup.SnapshotMetaKey(goodSource, good), good, 2, f.window(f.backupT0))
	f.incomplete(badSource, bad)
	f.deleteDisk(f.restoreSnapshot(bad, f.size, 1))
	f.deleteDisk(f.restoreSnapshot(good, f.size, 2))
	f.holdFault(badSource, bad, op, true)
	f.rules()
	f.backupReady(backup.SnapshotMetaKey(badSource, bad), bad, 1, f.window(f.backupT0))
	f.deleteDisk(badSource)
	f.deleteDisk(goodSource)
	f.copies(bad, badSource, 1, f.size)
	f.copies(good, goodSource, 2, f.size)
}
func TestBackupFollowerPermanentOutageKeepsPrimary(t *testing.T) {
	f := newFixture(t, dm.DiskKind_DISK_KIND_SSD)
	f.calibrate()
	source, id := f.id("source"), f.id("snapshot")
	f.empty(source, f.size)
	f.fill(source, 1)
	r := faultRule{ID: f.id("follower-down"), Route: "backup", Mode: "error"}
	f.rules(r)
	op, err := f.createSnapshot(id, source)
	f.success(op, err, f.window(f.createT0))
	f.hit(r.ID, f.window(f.backupT0))
	deadline := time.Now().Add(maximum(60*time.Second, f.window(f.backupT0)))
	t.Logf("PERMANENT_FOLLOWER fixed observation=%v", time.Until(deadline))
	for time.Now().Before(deadline) {
		f.incomplete(source, id)
		time.Sleep(250 * time.Millisecond)
	}
	f.deleteDisk(source)
	f.deleteDisk(f.restoreSnapshot(id, f.size, 1))
	f.incomplete(source, id)
	// Removal only cleans up the test; no successful follower result is claimed during outage.
	f.rules()
}
