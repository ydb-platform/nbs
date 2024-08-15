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

type getFilesystem struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	filesystemID string
}

func (c *getFilesystem) run() error {
	ctx := newContext(c.clientConfig)

	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	image, err := resourceStorage.GetFilesystemMeta(ctx, c.filesystemID)
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

func newGetFilesystemCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &getFilesystem{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "get",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.filesystemID, "id", "", "filesystem ID; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type listFilesystems struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	folderID     string
}

func (c *listFilesystems) run() error {
	ctx := newContext(c.clientConfig)

	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	ids, err := resourceStorage.ListFilesystems(ctx, c.folderID, time.Now())
	if err != nil {
		return err
	}

	fmt.Println(strings.Join(ids, "\n"))

	return nil
}

func newListFilesystemsCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &listFilesystems{
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
		"ID of folder where filesystems are located; optional",
	)
	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type createFilesystem struct {
	clientConfig *client_config.ClientConfig
	zoneID       string
	filesystemID string
	cloudID      string
	folderID     string
	blockSize    int64
	size         int64
}

func (c *createFilesystem) run() error {
	ctx := newContext(c.clientConfig)

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &disk_manager.CreateFilesystemRequest{
		FilesystemId: &disk_manager.FilesystemId{
			ZoneId:       c.zoneID,
			FilesystemId: c.filesystemID,
		},
		CloudId:   c.cloudID,
		FolderId:  c.folderID,
		BlockSize: c.blockSize,
		Size:      c.size,
	}

	resp, err := client.CreateFilesystem(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newCreateFilesystemCmd(clientConfig *client_config.ClientConfig) *cobra.Command {
	c := &createFilesystem{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use: "create",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().Int64Var(&c.size, "size", 0, "size of the filesystem in bytes; required")
	if err := cmd.MarkFlagRequired("size"); err != nil {
		log.Fatalf("Error setting flag size as required: %v", err)
	}

	cmd.Flags().StringVar(&c.zoneID, "zone-id", "", "zone ID in which to create filesystem; required")
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.filesystemID, "id", "", "filesystem ID; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	cmd.Flags().Int64Var(&c.blockSize, "block-size", 0, "block size in bytes. 0 - use default")

	cmd.Flags().StringVar(&c.cloudID, "cloud-id", "", "cloud ID of the filesystem owner; required")
	if err := cmd.MarkFlagRequired("cloud-id"); err != nil {
		log.Fatalf("Error setting flag cloud-id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.folderID, "folder-id", "", "folder ID of the filesystem owner; required")
	if err := cmd.MarkFlagRequired("folder-id"); err != nil {
		log.Fatalf("Error setting flag folder-id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type deleteFilesystem struct {
	clientConfig *client_config.ClientConfig
	zoneID       string
	filesystemID string
}

func (c *deleteFilesystem) run() error {
	ctx := newContext(c.clientConfig)

	err := requestConfirmation("filesystem", c.filesystemID)
	if err != nil {
		return err
	}

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &disk_manager.DeleteFilesystemRequest{
		FilesystemId: &disk_manager.FilesystemId{
			ZoneId:       c.zoneID,
			FilesystemId: c.filesystemID,
		},
	}

	resp, err := client.DeleteFilesystem(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newDeleteFilesystemCmd(clientConfig *client_config.ClientConfig) *cobra.Command {
	c := &deleteFilesystem{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use: "delete",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.zoneID, "zone-id", "", "zone ID where filesystem is located; required")
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.filesystemID, "id", "", "filesystem ID; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type resizeFilesystem struct {
	clientConfig *client_config.ClientConfig
	zoneID       string
	filesystemID string
	size         int64
}

func (c *resizeFilesystem) run() error {
	ctx := newContext(c.clientConfig)

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &disk_manager.ResizeFilesystemRequest{
		FilesystemId: &disk_manager.FilesystemId{
			ZoneId:       c.zoneID,
			FilesystemId: c.filesystemID,
		},
		Size: c.size,
	}

	resp, err := client.ResizeFilesystem(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newResizeFilesystemCmd(clientConfig *client_config.ClientConfig) *cobra.Command {
	c := &resizeFilesystem{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use: "resize",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().Int64Var(&c.size, "size", 0, "new size of the filesystem in bytes; required")
	if err := cmd.MarkFlagRequired("size"); err != nil {
		log.Fatalf("Error setting flag size as required: %v", err)
	}

	cmd.Flags().StringVar(&c.zoneID, "zone-id", "", "zone ID where filesystem is located; required")
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.filesystemID, "id", "", "filesystem ID; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

func newFilesystemCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmd := &cobra.Command{
		Use: "filesystem",
	}

	cmd.AddCommand(
		newGetFilesystemCmd(clientConfig, serverConfig),
		newListFilesystemsCmd(clientConfig, serverConfig),
		newCreateFilesystemCmd(clientConfig),
		newDeleteFilesystemCmd(clientConfig),
		newResizeFilesystemCmd(clientConfig),
	)

	return cmd
}
