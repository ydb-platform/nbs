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

type getSnapshot struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	snapshotID   string
}

func (c *getSnapshot) run() error {
	ctx := newContext(c.clientConfig)

	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	image, err := resourceStorage.GetSnapshotMeta(ctx, c.snapshotID)
	if err != nil {
		return err
	}

	j, err := json.Marshal(image)
	if err != nil {
		return err
	}

	fmt.Println(string(j))

	return nil
}

func newGetSnapshotCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &getSnapshot{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "get",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.snapshotID, "id", "", "ID of snapshot to get; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type listSnapshots struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	folderID     string
}

func (c *listSnapshots) run() error {
	ctx := newContext(c.clientConfig)

	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	ids, err := resourceStorage.ListSnapshots(ctx, c.folderID, time.Now())
	if err != nil {
		return err
	}

	fmt.Println(strings.Join(ids, "\n"))

	return nil
}

func newListSnapshotsCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &listSnapshots{
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
		"ID of folder where snapshots are located; optional",
	)
	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type createSnapshot struct {
	clientConfig  *client_config.ClientConfig
	zoneID        string
	srcDiskID     string
	dstSnapshotID string
	folderID      string
}

func (c *createSnapshot) run() error {
	ctx := newContext(c.clientConfig)

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: c.zoneID,
			DiskId: c.srcDiskID,
		},
		SnapshotId: c.dstSnapshotID,
		FolderId:   c.folderID,
	}

	resp, err := client.CreateSnapshot(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newCreateSnapshotCmd(clientConfig *client_config.ClientConfig) *cobra.Command {
	c := &createSnapshot{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use: "create",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.zoneID, "zone-id", "", "zone ID in which disk is located; required")
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.srcDiskID, "src-disk-id", "", "ID of disk to create snapshot from; required")
	if err := cmd.MarkFlagRequired("src-disk-id"); err != nil {
		log.Fatalf("Error setting flag src-disk-id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.dstSnapshotID, "id", "", "ID of snapshot to create; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.folderID, "folder-id", "", "folder ID of the snapshot owner; required")
	if err := cmd.MarkFlagRequired("folder-id"); err != nil {
		log.Fatalf("Error setting flag folder-id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type deleteSnapshot struct {
	clientConfig *client_config.ClientConfig
	snapshotID   string
}

func (c *deleteSnapshot) run() error {
	ctx := newContext(c.clientConfig)

	err := requestConfirmation("snapshot", c.snapshotID)
	if err != nil {
		return err
	}

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &disk_manager.DeleteSnapshotRequest{
		SnapshotId: c.snapshotID,
	}

	resp, err := client.DeleteSnapshot(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newDeleteSnapshotCmd(clientConfig *client_config.ClientConfig) *cobra.Command {
	c := &deleteSnapshot{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use: "delete",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.snapshotID, "id", "", "ID of snapshot to delete; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

func newSnapshotsCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmd := &cobra.Command{
		Use:     "snapshots",
		Aliases: []string{"snapshot"},
	}

	cmd.AddCommand(
		newGetSnapshotCmd(clientConfig, serverConfig),
		newListSnapshotsCmd(clientConfig, serverConfig),
		newCreateSnapshotCmd(clientConfig),
		newDeleteSnapshotCmd(clientConfig),
	)

	return cmd
}
