package admin

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"google.golang.org/protobuf/encoding/protojson"
)

////////////////////////////////////////////////////////////////////////////////

type rebaseOverlayDisk struct {
	config           *client_config.ClientConfig
	zoneID           string
	diskID           string
	targetBaseDiskID string
	async            bool
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

	if c.async {
		return nil
	}

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newRebaseOverlayDiskCmd(config *client_config.ClientConfig) *cobra.Command {
	c := &rebaseOverlayDisk{
		config: config,
	}

	cmd := &cobra.Command{
		Use:     "rebase-overlay-disk",
		Aliases: []string{"rebase_overlay_disk"},
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

	cmd.Flags().BoolVar(
		&c.async,
		"async",
		false,
		"do not wait for task ending",
	)

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type releaseBaseDisk struct {
	config        *client_config.ClientConfig
	zoneID        string
	overlayDiskID string
}

func (c *releaseBaseDisk) run() error {
	ctx := newContext(c.config)

	err := requestConfirmation("overlay disk", c.overlayDiskID)
	if err != nil {
		return err
	}

	client, err := internal_client.NewPrivateClientForCLI(ctx, c.config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &api.ReleaseBaseDiskRequest{
		DiskId: &disk_manager.DiskId{
			ZoneId: c.zoneID,
			DiskId: c.overlayDiskID,
		},
	}

	resp, err := client.ReleaseBaseDisk(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newReleaseBaseDiskCmd(config *client_config.ClientConfig) *cobra.Command {
	c := &releaseBaseDisk{
		config: config,
	}

	cmd := &cobra.Command{
		Use:     "release-base-disk",
		Aliases: []string{"release_base_disk"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.zoneID,
		"zone-id",
		"",
		"overlay disk zone ID where disk is located; required",
	)
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.overlayDiskID,
		"overlay-disk-id",
		"",
		"overlay disk ID; required",
	)
	if err := cmd.MarkFlagRequired("overlay-disk-id"); err != nil {
		log.Fatalf("Error setting flag overlay-disk-id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type retireBaseDisk struct {
	config        *client_config.ClientConfig
	baseDiskID    string
	srcDiskZoneID string
	srcDiskID     string
	async         bool
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

	if c.async {
		return nil
	}

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newRetireBaseDiskCmd(config *client_config.ClientConfig) *cobra.Command {
	c := &retireBaseDisk{
		config: config,
	}

	cmd := &cobra.Command{
		Use:     "retire-base-disk",
		Aliases: []string{"retire_base_disk"},
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

	cmd.Flags().BoolVar(
		&c.async,
		"async",
		false,
		"do not wait for task ending",
	)

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type retireBaseDisks struct {
	config           *client_config.ClientConfig
	imageID          string
	zoneID           string
	useBaseDiskAsSrc bool
	useImageSize     uint64
	async            bool
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
		UseImageSize:     c.useImageSize,
	}

	resp, err := client.RetireBaseDisks(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	if c.async {
		return nil
	}

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newRetireBaseDisksCmd(config *client_config.ClientConfig) *cobra.Command {
	c := &retireBaseDisks{
		config: config,
	}

	cmd := &cobra.Command{
		Use:     "retire-base-disks",
		Aliases: []string{"retire_base_disks"},
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

	cmd.Flags().Uint64Var(
		&c.useImageSize,
		"use-image-size",
		0,
		"size of new base disks will be equal to this parameter if it is set",
	)

	cmd.Flags().BoolVar(
		&c.async,
		"async",
		false,
		"do not wait for task ending",
	)

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type optimizeBaseDisks struct {
	config *client_config.ClientConfig
	async  bool
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

	if c.async {
		return nil
	}

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newOptimizeBaseDisksCmd(config *client_config.ClientConfig) *cobra.Command {
	c := &optimizeBaseDisks{
		config: config,
	}

	cmd := &cobra.Command{
		Use:     "optimize-base-disks",
		Aliases: []string{"optimize_base_disks"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().BoolVar(
		&c.async,
		"async",
		false,
		"do not wait for task ending",
	)

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
		Use:     "configure-pool",
		Aliases: []string{"configure_pool"},
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
		Use:     "delete-pool",
		Aliases: []string{"delete_pool"},
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
		Use:     "get-alive-nodes",
		Aliases: []string{"get_alive_nodes"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type getCheckpointSizeCmd struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	diskID       string
	zoneID       string
	checkpointID string
}

func (c *getCheckpointSizeCmd) run() error {
	ctx := newContext(c.clientConfig)
	nbsFactory, err := nbs.NewFactory(
		ctx,
		c.serverConfig.NbsConfig,
		metrics.NewEmptyRegistry(),
		metrics.NewEmptyRegistry(),
	)
	if err != nil {
		logging.Error(ctx, "Failed to create nbs factory: %v", err)
		return err
	}

	client, err := nbsFactory.GetClient(ctx, c.zoneID)
	if err != nil {
		return err
	}

	var checkpointSize uint64
	err = client.GetCheckpointSize(
		ctx,
		func(blockIndex uint64, result uint64) error {
			checkpointSize = result
			return nil
		},
		c.diskID,
		c.checkpointID,
		0, // milestoneBlockIndex
		0, // milestoneCheckpointSize
	)
	if err != nil {
		return err
	}

	fmt.Printf("%v\n", checkpointSize)
	return nil
}

func newGetCheckpointSizeCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &getCheckpointSizeCmd{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use:     "get-checkpoint-size",
		Aliases: []string{"get_checkpoint_size"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.diskID, "disk-id", "", "disk ID; required")
	if err := cmd.MarkFlagRequired("disk-id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.zoneID, "zone-id", "", "zone ID where disk is located;")
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.checkpointID, "checkpoint-id", "", "checkpoint id")
	if err := cmd.MarkFlagRequired("checkpoint-id"); err != nil {
		log.Fatalf("Error setting flag checkpoint-id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type finishExternalFilesystemCreation struct {
	config                     *client_config.ClientConfig
	filesystemID               string
	externalStorageClusterName string
}

func (c *finishExternalFilesystemCreation) run() error {
	ctx := newContext(c.config)

	client, err := internal_client.NewPrivateClientForCLI(ctx, c.config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	return client.FinishExternalFilesystemCreation(
		getRequestContext(ctx),
		&api.FinishExternalFilesystemCreationRequest{
			FilesystemId:               c.filesystemID,
			ExternalStorageClusterName: c.externalStorageClusterName,
		},
	)
}

func newFinishExternalFilesystemCreation(
	config *client_config.ClientConfig,
) *cobra.Command {

	c := &finishExternalFilesystemCreation{
		config: config,
	}

	cmd := &cobra.Command{
		Use:     "finish-external-filesystem-creation",
		Aliases: []string{"finish_external_filesystem_creation"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.filesystemID,
		"filesystem-id",
		"",
		"filesystem ID to operate on; required",
	)
	if err := cmd.MarkFlagRequired("filesystem-id"); err != nil {
		log.Fatalf("Error setting flag filesystem-id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.externalStorageClusterName,
		"external-storage-cluster-name",
		"",
		"external storage cluster name where filesystem's data is located; required",
	)
	if err := cmd.MarkFlagRequired("external-storage-cluster-name"); err != nil {
		log.Fatalf(
			"Error setting flag external-storage-cluster-name as required: %v",
			err,
		)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type finishExternalFilesystemDeletion struct {
	config       *client_config.ClientConfig
	filesystemID string
}

func (c *finishExternalFilesystemDeletion) run() error {
	ctx := newContext(c.config)

	client, err := internal_client.NewPrivateClientForCLI(ctx, c.config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	return client.FinishExternalFilesystemDeletion(
		getRequestContext(ctx),
		&api.FinishExternalFilesystemDeletionRequest{
			FilesystemId: c.filesystemID,
		},
	)
}

func newFinishExternalFilesystemDeletion(
	config *client_config.ClientConfig,
) *cobra.Command {

	c := &finishExternalFilesystemDeletion{
		config: config,
	}

	cmd := &cobra.Command{
		Use:     "finish-external-filesystem-deletion",
		Aliases: []string{"finish_external_filesystem_deletion"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.filesystemID,
		"filesystem-id",
		"",
		"filesystem ID to operate on; required",
	)
	if err := cmd.MarkFlagRequired("filesystem-id"); err != nil {
		log.Fatalf("Error setting flag filesystem-id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

func newPrivateCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmd := &cobra.Command{
		Use: "private",
	}

	cmd.AddCommand(
		newReleaseBaseDiskCmd(clientConfig),
		newRebaseOverlayDiskCmd(clientConfig),
		newRetireBaseDiskCmd(clientConfig),
		newRetireBaseDisksCmd(clientConfig),
		newOptimizeBaseDisksCmd(clientConfig),
		newConfigurePoolCmd(clientConfig),
		newDeletePoolCmd(clientConfig),
		newGetAliveNodesCmd(clientConfig),
		newGetCheckpointSizeCmd(clientConfig, serverConfig),
		newFinishExternalFilesystemCreation(clientConfig),
		newFinishExternalFilesystemDeletion(clientConfig),
	)

	return cmd
}
