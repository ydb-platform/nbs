package admin

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/spf13/cobra"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
)

////////////////////////////////////////////////////////////////////////////////

type getFilesystemSnapshot struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	snapshotID   string
}

func (c *getFilesystemSnapshot) run() error {
	ctx := newContext(c.clientConfig)

	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	snapshot, err := resourceStorage.GetFilesystemSnapshotMeta(ctx, c.snapshotID)
	if err != nil {
		return err
	}

	j, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}

	fmt.Println(string(j))

	return nil
}

func newGetFilesystemSnapshotCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &getFilesystemSnapshot{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "get",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.snapshotID,
		"id",
		"",
		"ID of filesystem snapshot to get; required",
	)
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type listFilesystemSnapshots struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	folderID     string
}

func (c *listFilesystemSnapshots) run() error {
	ctx := newContext(c.clientConfig)

	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	ids, err := resourceStorage.ListFilesystemSnapshots(
		ctx,
		c.folderID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	fmt.Println(strings.Join(ids, "\n"))

	return nil
}

func newListFilesystemSnapshotsCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &listFilesystemSnapshots{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "list",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.folderID,
		"folder-id",
		"",
		"ID of folder where filesystem snapshots are located; optional",
	)

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type createFilesystemSnapshot struct {
	clientConfig *client_config.ClientConfig
	zoneID       string
	filesystemID string
	snapshotID   string
	folderID     string
}

func (c *createFilesystemSnapshot) run() error {
	ctx := newContext(c.clientConfig)

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	operation, err := client.CreateFilesystemSnapshot(
		getRequestContext(ctx),
		&disk_manager.CreateFilesystemSnapshotRequest{
			Src: &disk_manager.FilesystemId{
				ZoneId:       c.zoneID,
				FilesystemId: c.filesystemID,
			},
			FilesystemSnapshotId: c.snapshotID,
			FolderId:             c.folderID,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", operation.Id)

	return internal_client.WaitOperation(ctx, client, operation.Id)
}

func newCreateFilesystemSnapshotCmd(
	clientConfig *client_config.ClientConfig,
) *cobra.Command {

	c := &createFilesystemSnapshot{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use: "create",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.zoneID,
		"zone-id",
		"",
		"zone ID where the source filesystem is located; required",
	)
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.filesystemID,
		"src-filesystem-id",
		"",
		"ID of filesystem to create snapshot from; required",
	)
	if err := cmd.MarkFlagRequired("src-filesystem-id"); err != nil {
		log.Fatalf("Error setting flag src-filesystem-id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.snapshotID,
		"id",
		"",
		"ID of filesystem snapshot to create; required",
	)
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.folderID,
		"folder-id",
		"",
		"folder ID of the filesystem snapshot owner; required",
	)
	if err := cmd.MarkFlagRequired("folder-id"); err != nil {
		log.Fatalf("Error setting flag folder-id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type deleteFilesystemSnapshot struct {
	clientConfig *client_config.ClientConfig
	snapshotID   string
}

func (c *deleteFilesystemSnapshot) run() error {
	ctx := newContext(c.clientConfig)

	err := requestConfirmation("filesystem snapshot", c.snapshotID)
	if err != nil {
		return err
	}

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	operation, err := client.DeleteFilesystemSnapshot(
		getRequestContext(ctx),
		&disk_manager.DeleteFilesystemSnapshotRequest{
			FilesystemSnapshotId: c.snapshotID,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", operation.Id)

	return internal_client.WaitOperation(ctx, client, operation.Id)
}

func newDeleteFilesystemSnapshotCmd(
	clientConfig *client_config.ClientConfig,
) *cobra.Command {

	c := &deleteFilesystemSnapshot{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use: "delete",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.snapshotID,
		"id",
		"",
		"ID of filesystem snapshot to delete; required",
	)
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

func newFilesystemSnapshotsCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmd := &cobra.Command{
		Use: "filesystem-snapshots",
		Aliases: []string{
			"filesystem-snapshot",
			"filesystem_snapshot",
			"filesystem_snapshots",
		},
	}

	cmd.AddCommand(
		newGetFilesystemSnapshotCmd(clientConfig, serverConfig),
		newListFilesystemSnapshotsCmd(clientConfig, serverConfig),
		newCreateFilesystemSnapshotCmd(clientConfig),
		newDeleteFilesystemSnapshotCmd(clientConfig),
	)

	return cmd
}
