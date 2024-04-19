package admin

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	"google.golang.org/protobuf/encoding/protojson"
)

////////////////////////////////////////////////////////////////////////////////

type rebaseOverlayDisk struct {
	config           *client_config.ClientConfig
	zoneID           string
	diskID           string
	targetBaseDiskID string
}

func (c *rebaseOverlayDisk) run() error {
	ctx := newContext(c.config)

	client, err := internal_client.NewPrivateClientForCLI(ctx, c.config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &api.RebaseOverlayDiskRequest{
		DiskId: &disk_manager.DiskId{
			ZoneId: c.zoneID,
			DiskId: c.diskID,
		},
		TargetBaseDiskId: c.targetBaseDiskID,
	}

	resp, err := client.RebaseOverlayDisk(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newRebaseOverlayDiskCmd(config *client_config.ClientConfig) *cobra.Command {
	c := &rebaseOverlayDisk{
		config: config,
	}

	cmd := &cobra.Command{
		Use: "rebase_overlay_disk",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.zoneID,
		"zone-id",
		"",
		"zone ID where disk is located; required",
	)
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.diskID, "id", "", "disk ID; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.targetBaseDiskID,
		"target-base-disk-id",
		"",
		"ID of disk to rebase onto; required",
	)
	if err := cmd.MarkFlagRequired("target-base-disk-id"); err != nil {
		log.Fatalf("Error setting flag target-base-disk-id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type retireBaseDisk struct {
	config        *client_config.ClientConfig
	baseDiskID    string
	srcDiskZoneID string
	srcDiskID     string
}

func (c *retireBaseDisk) run() error {
	ctx := newContext(c.config)

	client, err := internal_client.NewPrivateClientForCLI(ctx, c.config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &api.RetireBaseDiskRequest{
		BaseDiskId: c.baseDiskID,
		SrcDiskId: &disk_manager.DiskId{
			ZoneId: c.srcDiskZoneID,
			DiskId: c.srcDiskID,
		},
	}

	resp, err := client.RetireBaseDisk(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newRetireBaseDiskCmd(config *client_config.ClientConfig) *cobra.Command {
	c := &retireBaseDisk{
		config: config,
	}

	cmd := &cobra.Command{
		Use: "retire_base_disk",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.baseDiskID,
		"id",
		"",
		"ID of base disk for retiring; required",
	)
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.srcDiskZoneID,
		"src-disk-zone-id",
		"",
		"zone ID where source disk is located",
	)
	cmd.Flags().StringVar(&c.srcDiskID, "src-disk-id", "", "id of source disk")

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type retireBaseDisks struct {
	config           *client_config.ClientConfig
	imageID          string
	zoneID           string
	useBaseDiskAsSrc bool
}

func (c *retireBaseDisks) run() error {
	ctx := newContext(c.config)

	client, err := internal_client.NewPrivateClientForCLI(ctx, c.config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &api.RetireBaseDisksRequest{
		ImageId:          c.imageID,
		ZoneId:           c.zoneID,
		UseBaseDiskAsSrc: c.useBaseDiskAsSrc,
	}

	resp, err := client.RetireBaseDisks(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newRetireBaseDisksCmd(config *client_config.ClientConfig) *cobra.Command {
	c := &retireBaseDisks{
		config: config,
	}

	cmd := &cobra.Command{
		Use: "retire_base_disks",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.imageID,
		"image-id",
		"",
		"ID of image, base disks created from this image should be retired; required",
	)
	if err := cmd.MarkFlagRequired("image-id"); err != nil {
		log.Fatalf("Error setting flag image-id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.zoneID,
		"zone-id", "", "zone ID, where base disks are located; required",
	)
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	cmd.Flags().BoolVar(
		&c.useBaseDiskAsSrc,
		"use-base-disk-as-src",
		false,
		"enables feature that uses base disk as src when doing retire",
	)

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type optimizeBaseDisks struct {
	config *client_config.ClientConfig
}

func (c *optimizeBaseDisks) run() error {
	ctx := newContext(c.config)

	client, err := internal_client.NewPrivateClientForCLI(ctx, c.config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	resp, err := client.OptimizeBaseDisks(getRequestContext(ctx))
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newOptimizeBaseDisksCmd(config *client_config.ClientConfig) *cobra.Command {
	c := &optimizeBaseDisks{
		config: config,
	}

	cmd := &cobra.Command{
		Use: "optimize_base_disks",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type configurePool struct {
	config       *client_config.ClientConfig
	imageID      string
	zoneID       string
	capacity     uint64
	useImageSize bool
}

func (c *configurePool) run() error {
	ctx := newContext(c.config)

	client, err := internal_client.NewPrivateClientForCLI(ctx, c.config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	resp, err := client.ConfigurePool(
		getRequestContext(ctx),
		&api.ConfigurePoolRequest{
			ImageId:      c.imageID,
			ZoneId:       c.zoneID,
			Capacity:     int64(c.capacity),
			UseImageSize: c.useImageSize,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newConfigurePoolCmd(config *client_config.ClientConfig) *cobra.Command {
	c := &configurePool{
		config: config,
	}

	cmd := &cobra.Command{
		Use: "configure_pool",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.imageID,
		"image-id",
		"",
		"image ID to configure pool for; required",
	)
	if err := cmd.MarkFlagRequired("image-id"); err != nil {
		log.Fatalf("Error setting flag image-id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.zoneID,
		"zone-id",
		"",
		"zone-id ID where pool should be located; required",
	)
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	cmd.Flags().Uint64Var(
		&c.capacity,
		"capacity",
		0,
		"capacity of pool measured in disks; required",
	)
	if err := cmd.MarkFlagRequired("capacity"); err != nil {
		log.Fatalf("Error setting flag capacity as required: %v", err)
	}

	cmd.Flags().BoolVar(
		&c.useImageSize,
		"use-image-size",
		false,
		"enables feature that allocates base disks according to image size",
	)

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type deletePool struct {
	config  *client_config.ClientConfig
	imageID string
	zoneID  string
}

func (c *deletePool) run() error {
	ctx := newContext(c.config)

	client, err := internal_client.NewPrivateClientForCLI(ctx, c.config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	resp, err := client.DeletePool(
		getRequestContext(ctx),
		&api.DeletePoolRequest{
			ImageId: c.imageID,
			ZoneId:  c.zoneID,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newDeletePoolCmd(config *client_config.ClientConfig) *cobra.Command {
	c := &deletePool{
		config: config,
	}

	cmd := &cobra.Command{
		Use: "delete_pool",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.imageID,
		"image-id",
		"",
		"image ID to delete pool for; required",
	)
	if err := cmd.MarkFlagRequired("image-id"); err != nil {
		log.Fatalf("Error setting flag image-id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.zoneID,
		"zone-id",
		"",
		"zone ID where pool is located; required",
	)
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type getAliveNodes struct {
	config *client_config.ClientConfig
}

func (l *getAliveNodes) run() error {
	ctx := newContext(l.config)

	client, err := internal_client.NewPrivateClientForCLI(ctx, l.config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	resp, err := client.GetAliveNodes(ctx)
	if err != nil {
		return err
	}

	for _, node := range resp.GetNodes() {
		nodeJSON, err := protojson.Marshal(node)
		if err != nil {
			return err
		}
		fmt.Println(string(nodeJSON))
	}
	return nil
}

func newGetAliveNodesCmd(config *client_config.ClientConfig) *cobra.Command {
	c := &getAliveNodes{
		config: config,
	}

	cmd := &cobra.Command{
		Use: "get_alive_nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

func newPrivateCmd(config *client_config.ClientConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use: "private",
	}

	cmd.AddCommand(
		newRebaseOverlayDiskCmd(config),
		newRetireBaseDiskCmd(config),
		newRetireBaseDisksCmd(config),
		newOptimizeBaseDisksCmd(config),
		newConfigurePoolCmd(config),
		newDeletePoolCmd(config),
		newGetAliveNodesCmd(config),
	)

	return cmd
}
