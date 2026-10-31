package admin

import (
	"fmt"

	"github.com/spf13/cobra"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
)

type transferVolumeViaLink struct {
	config       *client_config.ClientConfig
	srcDiskID    string
	targetCellID string
	zoneID       string
	async        bool
}

func (c *transferVolumeViaLink) run() error {
	if c.srcDiskID == "" {
		return fmt.Errorf("source disk ID is required")
	}
	ctx := newContext(c.config)
	client, err := internal_client.NewClient(ctx, c.config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()
	operation, err := client.TransferVolumeViaLink(getRequestContext(ctx), &disk_manager.TransferVolumeViaLinkRequest{
		SrcDiskId: c.srcDiskID, DstCellId: c.targetCellID, ZoneId: c.zoneID,
	})
	if err != nil {
		return err
	}
	fmt.Printf("Operation: %s\n", operation.Id)
	if c.async {
		return nil
	}
	return internal_client.WaitOperation(ctx, client, operation.Id)
}

func newTransferVolumeViaLinkCmd(config *client_config.ClientConfig) *cobra.Command {
	c := &transferVolumeViaLink{config: config}
	cmd := &cobra.Command{
		Use:   "transfer-volume-via-link",
		Short: "Transfer a managed disk via an NBS link; NBS deletes the source volume",
		RunE:  func(cmd *cobra.Command, args []string) error { return c.run() },
	}
	cmd.Flags().StringVar(&c.srcDiskID, "src-disk-id", "", "Logical source disk ID; required")
	cmd.Flags().StringVar(&c.targetCellID, "dst-cell-id", "", "Exact destination cell ID; defaults to the source's current cell")
	cmd.Flags().StringVar(&c.zoneID, "zone-id", "", "Expected public zone; defaults to the source disk's zone")
	cmd.Flags().BoolVar(&c.async, "async", false, "Return without waiting for completion")
	if err := cmd.MarkFlagRequired("src-disk-id"); err != nil {
		panic(err)
	}
	return cmd
}
