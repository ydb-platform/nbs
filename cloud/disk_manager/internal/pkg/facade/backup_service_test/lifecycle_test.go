package tests

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	dm "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func (f *fixture) notReady(source, id string) {
	meta, err := testcommon.GetSnapshotMeta(f.t, f.ctx, id)
	require.NoError(f.t, err)
	if meta != nil {
		require.False(f.t, meta.Ready, "failed snapshot must not be Ready")
	}
	f.incomplete(source, id)
}
func (f *fixture) failed(op *dm.Operation, err error, limit time.Duration) {
	f.t.Helper()
	if err != nil {
		return
	}
	terminal, waitErr := f.wait(op, limit)
	require.Error(f.t, waitErr)
	require.NotNil(f.t, terminal)
	require.True(f.t, terminal.Done, "timeout is not an explicit failure")
	require.NotNil(f.t, terminal.GetError())
}
func (f *fixture) cancel(op *dm.Operation, limit time.Duration) {
	deadline := time.Now().Add(limit)
	ctx, cancel := context.WithDeadline(f.ctx, deadline)
	defer cancel()
	_, err := f.dm.CancelOperation(ctx, &dm.CancelOperationRequest{OperationId: op.Id})
	require.NoError(f.t, err)
	terminal, err := f.wait(op, time.Until(deadline))
	require.Error(f.t, err)
	require.NotNil(f.t, terminal)
	require.True(f.t, terminal.Done)
	require.NotNil(f.t, terminal.GetError())
	require.Equal(f.t, int32(codes.Canceled), terminal.GetError().Code)
}
func (f *fixture) calibrateCancel(source string) {
	for trial := 0; trial < 3; trial++ {
		id := f.id("cancel-control")
		rule := faultRule{ID: f.id("cancel-barrier"), Route: "nbs", Method: "CreateCheckpoint", Mode: "gate"}
		f.rules(rule)
		op, err := f.createSnapshot(id, source)
		require.NoError(f.t, err)
		f.hit(rule.ID, 5*time.Minute)
		start := time.Now()
		f.cancel(op, 5*time.Minute)
		f.cancelT0 = maximum(f.cancelT0, time.Since(start))
		f.rules()
		f.notReady(source, id)
	}
	f.t.Logf("CANCEL_CALIBRATION T0=%v P=%v W=%v", f.cancelT0, f.period, f.window(f.cancelT0))
}
func TestBackupPermanentFailureAndCancelSSD(t *testing.T) {
	f := newFixture(t, dm.DiskKind_DISK_KIND_SSD)
	f.calibrate()
	source := f.id("disk")
	f.empty(source, f.size)
	f.fill(source, 1)
	f.calibrateCancel(source)
	id := f.id("unavailable")
	r := faultRule{ID: f.id("permanent-unavailable"), Route: "nbs", Method: "ReadBlocks", Mode: "gate"}
	f.rules(r)
	op, err := f.createSnapshot(id, source)
	require.NoError(t, err)
	f.hit(r.ID, f.window(f.createT0))
	current, err := f.wait(op, f.window(f.createT0))
	require.Error(t, err)
	require.NotNil(t, current)
	require.False(t, current.Done)
	f.cancel(op, f.window(f.cancelT0))
	f.rules()
	f.notReady(source, id)
	id = f.id("irreversible")
	r = faultRule{ID: f.id("create-fails"), Route: "nbs", Method: "CreateCheckpoint", Mode: "permanent"}
	f.rules(r)
	op, err = f.createSnapshot(id, source)
	require.NoError(t, err)
	f.hit(r.ID, f.window(f.createT0))
	f.failed(op, nil, f.window(f.createT0))
	sawApplicationError := false
	for _, event := range f.events() {
		if event.Rule == r.ID && event.Outcome == "injected-nbs-argument" {
			sawApplicationError = true
		}
	}
	require.True(t, sawApplicationError, "irreversible failure must be an NBS application error")
	f.rules()
	f.notReady(source, id)
	// Removal of the cause permits a new user request.
	id = f.id("after-recovery")
	op, err = f.createSnapshot(id, source)
	f.success(op, err, f.window(f.createT0))
	f.backupReady(backup.SnapshotMetaKey(source, id), id, 1, f.window(f.backupT0))
	f.deleteDisk(source)
	f.copies(id, source, 1, f.size)
}
func TestBackupDeleteAndResizeRacesSSD(t *testing.T) {
	f := newFixture(t, dm.DiskKind_DISK_KIND_SSD)
	f.calibrate()
	for _, method := range []string{"CreateCheckpoint", "ReadBlocks"} {
		source, id := f.id("delete-source"), f.id("delete-snapshot")
		f.empty(source, f.size)
		f.fill(source, 1)
		r := faultRule{ID: f.id("delete-barrier"), Route: "nbs", Method: method, Mode: "gate"}
		f.rules(r)
		op, err := f.createSnapshot(id, source)
		require.NoError(t, err)
		f.hit(r.ID, f.window(f.createT0))
		f.deleteDisk(source)
		f.rules()
		terminal, err := f.wait(op, f.window(f.createT0))
		require.NotNil(t, terminal)
		require.True(t, terminal.Done, "deletion race must have an explicit outcome")
		if err == nil {
			require.Equal(t, "ReadBlocks", method, "creation before checkpoint cannot succeed after source deletion")
			f.backupReady(backup.SnapshotMetaKey(source, id), id, 1, f.window(f.backupT0))
			f.copies(id, source, 1, f.size)
		} else {
			f.notReady(source, id)
		}
	}
	for _, during := range []bool{false, true} {
		source, id := f.id("resize-source"), f.id("resize-snapshot")
		f.empty(source, f.size)
		f.fill(source, 1)
		var op *dm.Operation
		var err error
		if during {
			r := faultRule{ID: f.id("resize-barrier"), Route: "nbs", Method: "ReadBlocks", Mode: "gate"}
			f.rules(r)
			op, err = f.createSnapshot(id, source)
			require.NoError(t, err)
			f.hit(r.ID, f.window(f.createT0))
		}
		resize, resizeErr := f.dm.ResizeDisk(f.req(), &dm.ResizeDiskRequest{DiskId: diskID(source), Size: int64(2 * f.size)})
		require.NoError(t, resizeErr)
		resized, resizeErr := f.wait(resize, f.window(f.createT0))
		serialized := false
		if resizeErr != nil {
			require.True(t, during)
			require.NotNil(t, resized)
			require.True(t, resized.Done)
			require.NotNil(t, resized.GetError())
			require.True(t, strings.Contains(resized.GetError().Message, "E_TRY_AGAIN") &&
				strings.Contains(resized.GetError().Message, "exclusive volume operation"), "%v", resizeErr)
			serialized = true
			t.Logf("RESIZE_RACE serialized by NBS: %v; snapshot must retain old size", resizeErr)
		}
		f.rules()
		if !during {
			op, err = f.createSnapshot(id, source)
		}
		f.success(op, err, f.window(f.createT0))
		reader := f.backupReady(backup.SnapshotMetaKey(source, id), id, 1, f.window(f.backupT0))
		if serialized {
			require.Equal(t, f.size, reader.size)
			// Retry after checkpoint copy is complete; new public request must grow the disk.
			retry, retryErr := f.dm.ResizeDisk(f.req(), &dm.ResizeDiskRequest{DiskId: diskID(source), Size: int64(2 * f.size)})
			f.success(retry, retryErr, f.window(f.createT0))
			f.verifyDisk(source, 2*f.size, 1)
		} else if during {
			require.Contains(t, []uint64{f.size, 2 * f.size}, reader.size)
		} else {
			require.Equal(t, 2*f.size, reader.size)
		}
		f.deleteDisk(source)
		f.copies(id, source, 1, reader.size)
	}
}
func TestBackupNRDCheckpointFailure(t *testing.T) {
	f := newFixture(t, dm.DiskKind_DISK_KIND_SSD_NONREPLICATED)
	f.calibrate()
	f.temporaryFailure("nbs", "ReadBlocks", "", "error")
	source, id := f.id("nrd-source"), f.id("nrd-lost-checkpoint")
	f.empty(source, f.size)
	f.fill(source, 1)
	r := faultRule{ID: f.id("checkpoint-loss-barrier"), Route: "nbs", Method: "ReadBlocks", Mode: "gate"}
	f.rules(r)
	op, err := f.createSnapshot(id, source)
	require.NoError(t, err)
	f.hit(r.ID, f.window(f.createT0))
	require.NoError(t, f.nbs.DeleteCheckpoint(f.ctx, source, id))
	f.rules()
	f.failed(op, nil, f.window(f.createT0))
	f.notReady(source, id)
	id = f.id("nrd-recovered")
	op, err = f.createSnapshot(id, source)
	f.success(op, err, f.window(f.createT0))
	f.backupReady(backup.SnapshotMetaKey(source, id), id, 1, f.window(f.backupT0))
	f.deleteDisk(source)
	f.copies(id, source, 1, f.size)
}
func TestBackupPublicIdempotencyAndInvalidSources(t *testing.T) {
	f := newFixture(t, dm.DiskKind_DISK_KIND_SSD)
	f.calibrate()
	source, id := f.id("idempotent-source"), f.id("idempotent-snapshot")
	f.empty(source, f.size)
	f.fill(source, 1)
	creds, err := credentials.NewClientTLSFromFile(os.Getenv("DISK_MANAGER_RECIPE_ROOT_CERTS_FILE"), "")
	require.NoError(t, err)
	lost := false
	accepted := ""
	connection, err := grpc.DialContext(f.ctx, "localhost:"+os.Getenv("DISK_MANAGER_RECIPE_DISK_MANAGER_PORT"),
		grpc.WithTransportCredentials(creds),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			err := invoke(ctx, method, req, reply, cc, opts...)
			if err == nil && !lost {
				lost = true
				accepted = reply.(*dm.Operation).Id
				return status.Error(codes.Unavailable, "test: response lost after server acceptance")
			}
			return err
		}))
	require.NoError(t, err)
	defer connection.Close()
	client := dm.NewSnapshotServiceClient(connection)
	request := &dm.CreateSnapshotRequest{Src: diskID(source), SnapshotId: id, FolderId: "backup-acceptance"}
	ctx := f.req()
	_, err = client.Create(ctx, request)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.NotEmpty(t, accepted)
	retry, err := client.Create(ctx, request)
	require.NoError(t, err)
	require.Equal(t, accepted, retry.Id)
	again, err := client.Create(ctx, request)
	require.NoError(t, err)
	require.Equal(t, retry.Id, again.Id)
	_, err = f.wait(retry, f.window(f.createT0))
	require.NoError(t, err)
	f.backupReady(backup.SnapshotMetaKey(source, id), id, 1, f.window(f.backupT0))
	// Same resource ID with a conflicting source must not overwrite it.
	bad, err := f.createSnapshot(id, "missing-source")
	f.failed(bad, err, f.window(f.createT0))
	invalid := f.id("invalid")
	bad, err = f.createSnapshot(invalid, "missing-source")
	f.failed(bad, err, f.window(f.createT0))
	f.notReady("missing-source", invalid)
	f.deleteDisk(source)
	f.copies(id, source, 1, f.size)
	for trial := 0; trial < 2; trial++ {
		op, err := f.dm.DeleteSnapshot(f.req(), &dm.DeleteSnapshotRequest{SnapshotId: id})
		f.success(op, err, f.window(f.deleteT0))
	}
}
