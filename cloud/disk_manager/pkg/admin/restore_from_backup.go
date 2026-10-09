package admin

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/util"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type restoreFromBackup struct {
	commandWithScheduler
	srcKind           string
	srcID             string
	srcDiskID         string
	dstZoneID         string
	dstDiskID         string
	dstEncryptionFile string
	expectedFolderID  string
}

func (c *restoreFromBackup) run() error {
	request := &protos.TransferFromBackupToDiskRequest{
		SrcId:     c.srcID,
		SrcDiskId: c.srcDiskID,
		DstDisk: &types.Disk{
			ZoneId: c.dstZoneID,
			DiskId: c.dstDiskID,
		},
		ExpectedFolderId: c.expectedFolderID,
	}
	switch c.srcKind {
	case "snapshot":
		request.SrcKind = protos.TransferFromBackupToDiskRequest_SNAPSHOT
		if len(c.srcDiskID) == 0 {
			return fmt.Errorf("--src-disk-id is required for snapshot backups")
		}
	case "image":
		request.SrcKind = protos.TransferFromBackupToDiskRequest_IMAGE
	default:
		return fmt.Errorf("--src-kind must be snapshot or image")
	}
	if len(c.srcID) == 0 || len(c.dstZoneID) == 0 || len(c.dstDiskID) == 0 {
		return fmt.Errorf(
			"--src-id, --dst-zone-id and --dst-disk-id must be nonempty",
		)
	}

	if len(c.dstEncryptionFile) != 0 {
		request.DstEncryption = &types.EncryptionDesc{}
		err := util.ParseProto(c.dstEncryptionFile, request.DstEncryption)
		if err != nil {
			return err
		}
	}

	err := c.init()
	if err != nil {
		return err
	}
	defer c.close()

	taskID, err := c.scheduler.ScheduleZonalTask(
		headers.SetIncomingIdempotencyKey(
			c.ctx,
			"dataplane.TransferFromBackupToDisk_"+generateID(),
		),
		"dataplane.TransferFromBackupToDisk",
		"",
		request.DstDisk.ZoneId,
		request,
	)
	if err != nil {
		return err
	}

	fmt.Printf("Task: %v\n", taskID)
	return nil
}

func newRestoreFromBackupCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &restoreFromBackup{
		commandWithScheduler: newCommandWithScheduler(
			clientConfig,
			serverConfig,
		),
	}
	cmd := &cobra.Command{
		Use:   "restore-from-backup",
		Short: "Schedule an S3 backup restore to an existing NBS disk",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.srcKind,
		"src-kind",
		"",
		"Backup source kind: snapshot or image; required",
	)
	cmd.Flags().StringVar(
		&c.srcID,
		"src-id",
		"",
		"Snapshot or image ID in the backup; required",
	)
	cmd.Flags().StringVar(
		&c.srcDiskID,
		"src-disk-id",
		"",
		"Original disk ID; required for snapshot backups",
	)
	cmd.Flags().StringVar(
		&c.dstZoneID,
		"dst-zone-id",
		"",
		"Exact destination zone or cell ID; required",
	)
	cmd.Flags().StringVar(
		&c.dstDiskID,
		"dst-disk-id",
		"",
		"Existing destination disk ID; required",
	)
	cmd.Flags().StringVar(
		&c.dstEncryptionFile,
		"dst-encryption",
		"",
		"Path to destination types.EncryptionDesc in text protobuf format",
	)
	cmd.Flags().StringVar(
		&c.expectedFolderID,
		"expected-folder-id",
		"",
		"Require the backup metadata to belong to this folder",
	)

	for _, flag := range []string{
		"src-kind",
		"src-id",
		"dst-zone-id",
		"dst-disk-id",
	} {
		if err := cmd.MarkFlagRequired(flag); err != nil {
			log.Fatalf("Error setting flag %v as required: %v", flag, err)
		}
	}

	return cmd
}
