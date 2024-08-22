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

type getImage struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	imageID      string
}

func (c *getImage) run() error {
	ctx := newContext(c.clientConfig)

	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	image, err := resourceStorage.GetImageMeta(ctx, c.imageID)
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

func newGetImageCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &getImage{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "get",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.imageID, "id", "", "ID of image to get; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type listImages struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	folderID     string
}

func (c *listImages) run() error {
	ctx := newContext(c.clientConfig)

	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	ids, err := resourceStorage.ListImages(ctx, c.folderID, time.Now())
	if err != nil {
		return err
	}

	fmt.Println(strings.Join(ids, "\n"))

	return nil
}

func newListImagesCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &listImages{
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
		"ID of folder where images are located; optional",
	)
	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type createImage struct {
	clientConfig  *client_config.ClientConfig
	srcURL        string
	srcImageID    string
	srcSnapshotID string
	srcDiskZoneID string
	srcDiskID     string
	dstImageID    string
	folderID      string
}

func (c *createImage) run() error {
	ctx := newContext(c.clientConfig)

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &disk_manager.CreateImageRequest{
		DstImageId: c.dstImageID,
		FolderId:   c.folderID,
	}

	if c.srcURL != "" {
		req.Src = &disk_manager.CreateImageRequest_SrcUrl{
			SrcUrl: &disk_manager.ImageUrl{
				Url: c.srcURL,
			},
		}
	} else if c.srcImageID != "" {
		req.Src = &disk_manager.CreateImageRequest_SrcImageId{
			SrcImageId: c.srcImageID,
		}
	} else if c.srcSnapshotID != "" {
		req.Src = &disk_manager.CreateImageRequest_SrcSnapshotId{
			SrcSnapshotId: c.srcSnapshotID,
		}
	} else if c.srcDiskID != "" {
		if c.srcDiskZoneID == "" {
			return fmt.Errorf("src-disk-zone-id should be defined")
		}

		req.Src = &disk_manager.CreateImageRequest_SrcDiskId{
			SrcDiskId: &disk_manager.DiskId{
				ZoneId: c.srcDiskZoneID,
				DiskId: c.srcDiskID,
			},
		}
	} else {
		return fmt.Errorf("one of src-url or src-image-id or src-snapshot-id or src-disk-id should be defined")
	}

	resp, err := client.CreateImage(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newCreateImageCmd(clientConfig *client_config.ClientConfig) *cobra.Command {
	c := &createImage{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use: "create",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.srcURL, "src-url", "", "URL to create image from")
	cmd.Flags().StringVar(&c.srcImageID, "src-image-id", "", "ID of image to create image from")
	cmd.Flags().StringVar(&c.srcSnapshotID, "src-snapshot-id", "", "ID of snapshot to create image from")
	cmd.Flags().StringVar(&c.srcDiskZoneID, "src-disk-zone-id", "", "ID of zone where source disk is located")
	cmd.Flags().StringVar(&c.srcDiskID, "src-disk-id", "", "ID of disk to create image from")

	cmd.Flags().StringVar(&c.dstImageID, "id", "", "ID of image to create; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.folderID, "folder-id", "", "folder ID of the image owner; required")
	if err := cmd.MarkFlagRequired("folder-id"); err != nil {
		log.Fatalf("Error setting flag folder-id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type deleteImage struct {
	clientConfig *client_config.ClientConfig
	imageID      string
}

func (c *deleteImage) run() error {
	ctx := newContext(c.clientConfig)

	err := requestConfirmation("image", c.imageID)
	if err != nil {
		return err
	}

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &disk_manager.DeleteImageRequest{
		ImageId: c.imageID,
	}

	resp, err := client.DeleteImage(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newDeleteImageCmd(clientConfig *client_config.ClientConfig) *cobra.Command {
	c := &deleteImage{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use: "delete",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.imageID, "id", "", "ID of image to delete; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

func newImagesCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "images",
		Aliases: []string{"image"},
	}

	cmd.AddCommand(
		newGetImageCmd(clientConfig, serverConfig),
		newListImagesCmd(clientConfig, serverConfig),
		newCreateImageCmd(clientConfig),
		newDeleteImageCmd(clientConfig),
	)

	return cmd
}
