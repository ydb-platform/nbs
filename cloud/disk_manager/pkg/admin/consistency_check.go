package admin

import (
	"github.com/spf13/cobra"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
)

////////////////////////////////////////////////////////////////////////////////

type consistencyCheckTask struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
}

func (t *consistencyCheckTask) run() error {
	ctx := newContext(t.clientConfig)

	poolsStorage, db, err := newPoolStorage(ctx, t.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	return poolsStorage.CheckConsistency(ctx)
}

func newConsistencyCheckTask(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &consistencyCheckTask{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "check",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

func newConsistencyCheckCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmd := &cobra.Command{
		Use: "consistency",
	}

	cmd.AddCommand(
		newConsistencyCheckTask(clientConfig, serverConfig),
	)

	return cmd
}
