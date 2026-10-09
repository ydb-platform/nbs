package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/compressor"
	snapshot_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

type crashProcess struct {
	PID                int    `json:"pid"`
	RestartTimingsFile string `json:"restart_timings_file"`
	RestartTriggerFile string `json:"restart_trigger_file"`
}

type crashFaultStatus struct {
	Active           bool   `json:"active"`
	Mode             string `json:"mode"`
	Hits             int    `json:"hits"`
	UpstreamAccepted int    `json:"upstream_accepted"`
	HeldAfterSuccess int    `json:"held_after_success"`
}

func crashFaultControl(ctx context.Context, method, path string, body []byte) (crashFaultStatus, error) {
	var status crashFaultStatus
	url := "http://127.0.0.1:" + os.Getenv("DISK_MANAGER_RECIPE_BACKUP_S3_CONTROL_PORT") + path
	request, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return status, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: time.Second}).Do(request)
	if err != nil {
		return status, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return status, fmt.Errorf("fault controller returned HTTP %d", response.StatusCode)
	}
	err = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&status)
	return status, err
}

// Nemesis is a Go process: its child may belong to any runtime OS thread.
// Inspect only this recipe process, never a host-wide process inventory.
func crashDPChildPID(parentPID int) (string, error) {
	root := fmt.Sprintf("/proc/%d/task", parentPID)
	threads, err := os.ReadDir(root)
	if err != nil {
		return "", err
	}
	children := make(map[string]bool)
	for _, thread := range threads {
		data, err := os.ReadFile(filepath.Join(root, thread.Name(), "children"))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		for _, pid := range strings.Fields(string(data)) {
			children[pid] = true
		}
	}
	if len(children) != 1 {
		return "", fmt.Errorf("expected one DP child of Nemesis, got %d", len(children))
	}
	for pid := range children {
		return pid, nil
	}
	return "", fmt.Errorf("DP child missing")
}

func crashRestartSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func requestDPRestart(process crashProcess) error {
	path := process.RestartTriggerFile
	if !filepath.IsAbs(path) {
		return fmt.Errorf("controlled Nemesis trigger must be an absolute path")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > 32 {
		return fmt.Errorf("invalid controlled Nemesis trigger file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	generation, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return err
	}
	if generation == ^uint64(0) {
		return fmt.Errorf("Nemesis trigger generation overflow")
	}
	// Atomic replacement avoids the reader observing an empty/truncated counter.
	temporary, err := os.CreateTemp(filepath.Dir(path), ".restart-trigger-*")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	if _, err = fmt.Fprintf(temporary, "%d\n", generation+1); err != nil {
		temporary.Close()
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporary.Name(), path)
}

// Controlled Nemesis does not restart spontaneously. After proving that an
// accepted PUT response is still held, request exactly one DP restart and
// require cmd.Wait/replacement while the fault remains enabled. No ambiguous
// experiment is retried or silently accepted.
func observeAcceptedPutAndDPCrash(ctx context.Context, process crashProcess, ready chan<- struct{}) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var originalPID string
	var originalRestartSize int64
	var lastNoHitsSample time.Time
	var restartDeadline time.Time
	restartRequested := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		sampleStarted := time.Now()
		beforePID, beforeErr := crashDPChildPID(process.PID)
		beforeSize, err := crashRestartSize(process.RestartTimingsFile)
		if err != nil {
			return err
		}
		status, err := crashFaultControl(ctx, http.MethodGet, "/status", nil)
		if err != nil {
			return err
		}
		afterPID, afterErr := crashDPChildPID(process.PID)
		afterSize, err := crashRestartSize(process.RestartTimingsFile)
		if err != nil {
			return err
		}
		stable := beforeErr == nil && afterErr == nil && beforePID == afterPID && beforeSize == afterSize
		if originalPID == "" {
			if !stable || status.Hits != 0 {
				continue
			}
			originalPID = afterPID
			originalRestartSize = afterSize
			lastNoHitsSample = sampleStarted
			close(ready)
			continue
		}
		if !restartRequested && stable && (afterPID != originalPID || afterSize != originalRestartSize) {
			return fmt.Errorf("DP restarted before the test requested its crash")
		}
		if status.Hits == 0 {
			lastNoHitsSample = sampleStarted
			continue
		}
		if !status.Active || status.Mode != "hold-after-success" {
			return fmt.Errorf("held PUT must remain active until a DP crash is observed")
		}
		// Bound from the last zero-hit sample, not merely from the successful
		// upstream response: upload time is part of the SDK's 30s call timeout.
		if restartDeadline.IsZero() {
			restartDeadline = lastNoHitsSample.Add(20 * time.Second)
		}
		if !time.Now().Before(restartDeadline) {
			return fmt.Errorf("no proven DP crash within 20s of the first chunk PUT")
		}
		if !restartRequested {
			if !stable || status.UpstreamAccepted == 0 || status.HeldAfterSuccess == 0 {
				continue
			}
			if err := requestDPRestart(process); err != nil {
				return err
			}
			restartRequested = true
			continue
		}
		if stable && afterPID != originalPID && afterSize > originalRestartSize {
			if _, err := os.Stat("/proc/" + originalPID); os.IsNotExist(err) {
				return nil
			} else if err != nil {
				return err
			}
		}
	}
}

func TestSnapshotServiceBackupRecoversAfterDPCrashWithAcceptedChunkPUT(t *testing.T) {
	ctx, cancel := context.WithTimeout(testcommon.NewContext(), 5*time.Minute)
	defer cancel()
	require.True(t, testcommon.IsNemesisEnabled())
	require.Equal(t, "true", os.Getenv("DISK_MANAGER_RECIPE_CONTROLLED_NEMESIS"))
	require.Equal(t, "30", os.Getenv("DISK_MANAGER_RECIPE_BACKUP_S3_CALL_TIMEOUT_SECONDS"))
	require.NotEmpty(t, os.Getenv("DISK_MANAGER_RECIPE_BACKUP_S3_CONTROL_PORT"))
	var byRole map[string][]crashProcess
	require.NoError(t, json.Unmarshal([]byte(os.Getenv("DISK_MANAGER_RECIPE_NEMESIS_PROCESSES_BY_ROLE")), &byRole))
	require.Len(t, byRole["dataplane"], 1, "one DP is required to attribute chunk PUT ownership")
	require.NotEmpty(t, byRole["dataplane"][0].RestartTriggerFile)
	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()
	s3, err := persistence.NewS3Client(
		"http://localhost:"+os.Getenv("DISK_MANAGER_RECIPE_BACKUP_S3_READ_PORT"), "test",
		persistence.S3Credentials{ID: "test", Secret: "test"},
		2*time.Second, metrics.NewEmptyRegistry(), 0, nil, nil,
	)
	require.NoError(t, err)
	require.NoError(t, s3.CreateBucket(ctx, "snapshot-backup"))
	reader, err := backup.NewS3(s3, "snapshot-backup", "recipe", "", nil)
	require.NoError(t, err)
	const size = 4 * 1024 * 1024
	diskID := t.Name()
	operation, err := client.CreateDisk(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateDiskRequest{
		Src:    &disk_manager.CreateDiskRequest_SrcEmpty{SrcEmpty: &empty.Empty{}},
		DiskId: &disk_manager.DiskId{DiskId: diskID, ZoneId: "zone-a"},
		Size:   size, BlockSize: 4096, Kind: disk_manager.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)
	require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
	nbs := testcommon.NewNbsTestingClient(t, ctx, "zone-a")
	session, err := nbs.MountRW(ctx, diskID, 0, 0, nil)
	require.NoError(t, err)
	expected := bytes.Repeat([]byte{0x5a}, size)
	require.NoError(t, session.Write(ctx, 0, expected))
	session.Close(ctx)

	watchCtx, stopWatch := context.WithTimeout(ctx, 2*time.Minute)
	defer stopWatch()
	ready := make(chan struct{})
	observed := make(chan error, 1)
	go func() { observed <- observeAcceptedPutAndDPCrash(watchCtx, byRole["dataplane"][0], ready) }()
	select {
	case <-ready:
	case err := <-observed:
		require.NoError(t, err)
	case <-watchCtx.Done():
		t.Fatal("could not observe the recipe DP process")
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cleanupCancel()
		_, err := crashFaultControl(cleanupCtx, http.MethodPost, "/reset", nil)
		require.NoError(t, err)
	})
	_, err = crashFaultControl(ctx, http.MethodPost, "/fault", []byte(`{"mode":"hold-after-success","method":"PUT","path_prefix":"/snapshot-backup/recipe/chunks/","ttl_seconds":120}`))
	require.NoError(t, err)
	snapshotID := t.Name() + "-snapshot"
	operation, err = client.CreateSnapshot(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateSnapshotRequest{
		Src:        &disk_manager.DiskId{DiskId: diskID, ZoneId: "zone-a"},
		SnapshotId: snapshotID, FolderId: "folder",
	})
	require.NoError(t, err)
	require.NoError(t, <-observed, "C01 requires proof of a DP crash while the accepted PUT response was held")
	_, err = reader.GetObject(ctx, backup.ChunkMapKey(snapshotID))
	require.ErrorContains(t, err, "s3 object not found", "no final map before the held chunk PUT is acknowledged")
	_, err = crashFaultControl(ctx, http.MethodPost, "/reset", nil)
	require.NoError(t, err)
	require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
	var chunkMap protos.BackupChunkMap
	require.Eventually(t, func() bool {
		object, err := reader.GetObject(ctx, backup.ChunkMapKey(snapshotID))
		return err == nil && proto.Unmarshal(object.Data, &chunkMap) == nil
	}, 2*time.Minute, time.Second)
	require.Len(t, chunkMap.ChunkIds, 1)
	require.NotEmpty(t, chunkMap.ChunkIds[0])
	chunk, err := reader.GetObject(ctx, backup.ChunkKey(chunkMap.ChunkIds[0]))
	require.NoError(t, err)
	compression := ""
	if chunk.Metadata["Compression"] != nil {
		compression = *chunk.Metadata["Compression"]
	}
	restored := make([]byte, size)
	require.NoError(t, compressor.Decompress(compression, chunk.Data, restored,
		snapshot_metrics.New(metrics.NewEmptyRegistry(), "backup-crash-test")))
	require.NotNil(t, chunk.Metadata["Checksum"])
	checksum, err := strconv.ParseUint(*chunk.Metadata["Checksum"], 10, 32)
	require.NoError(t, err)
	require.EqualValues(t, checksum, crc32.ChecksumIEEE(restored))
	require.True(t, bytes.Equal(expected, restored), "recovered backup must match the original bytes")
	metaObject, err := reader.GetObject(ctx, backup.SnapshotMetaKey(diskID, snapshotID))
	require.NoError(t, err)
	var meta backup.SnapshotMeta
	require.NoError(t, json.Unmarshal(metaObject.Data, &meta))
	require.Equal(t, snapshotID, meta.ID)
	require.EqualValues(t, size, meta.Size)
	operation, err = client.DeleteSnapshot(testcommon.GetRequestContext(t, ctx), &disk_manager.DeleteSnapshotRequest{SnapshotId: snapshotID})
	require.NoError(t, err)
	require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
	testcommon.DeleteDisk(t, ctx, client, diskID)
}
