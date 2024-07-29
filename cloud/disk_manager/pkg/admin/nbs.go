package admin

import (
	"fmt"
	"log"

	internal_auth "a.yandex-team.ru/cloud/disk_manager/internal/pkg/auth"
	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

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
	creds := internal_auth.NewCredentials(ctx, c.serverConfig.GetAuthConfig())
	nbsFactory, err := nbs.NewFactoryWithCreds(
		ctx,
		c.serverConfig.NbsConfig,
		creds,
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
	serverConfig *client_config.ServerConfig,
) *cobra.Command {
	c := &getCheckpointSizeCmd{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "get-checkpoint-size",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.diskID, "id", "", "disk ID; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.zoneID, "zone-id", "", "zone ID where disk is located;")
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.checkpointID, "checkpoint-id", "", "checkpoint id")

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

func newNbsCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmd := &cobra.Command{
		Use: "nbs",
	}

	cmd.AddCommand(
		newGetCheckpointSizeCmd(clientConfig, serverConfig),
	)

	return cmd
}
