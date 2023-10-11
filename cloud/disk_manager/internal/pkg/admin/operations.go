package admin

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
)

////////////////////////////////////////////////////////////////////////////////

type getOperation struct {
	config      *client_config.ClientConfig
	operationID string
}

func (c *getOperation) run() error {
	ctx := newContext(c.config)

	client, err := client.NewClient(ctx, c.config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	resp, err := client.GetOperation(
		getRequestContext(ctx),
		&disk_manager.GetOperationRequest{
			OperationId: c.operationID,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp)
	return nil
}

func newGetOperationCmd(config *client_config.ClientConfig) *cobra.Command {
	c := &getOperation{
		config: config,
	}

	cmd := &cobra.Command{
		Use: "get",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.operationID, "id", "", "ID of operation to get")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type cancelOperation struct {
	config      *client_config.ClientConfig
	operationID string
}

func (c *cancelOperation) run() error {
	ctx := newContext(c.config)

	client, err := client.NewClient(ctx, c.config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	resp, err := client.CancelOperation(
		getRequestContext(ctx),
		&disk_manager.CancelOperationRequest{
			OperationId: c.operationID,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp)
	return nil
}

func newCancelOperationCmd(
	config *client_config.ClientConfig,
) *cobra.Command {

	c := &cancelOperation{
		config: config,
	}

	cmd := &cobra.Command{
		Use: "cancel",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.operationID, "id", "", "ID of operation to cancel")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

func newOperationsCmd(config *client_config.ClientConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "operations",
		Aliases: []string{"operation"},
	}

	cmd.AddCommand(
		newGetOperationCmd(config),
		newCancelOperationCmd(config),
	)

	return cmd
}
