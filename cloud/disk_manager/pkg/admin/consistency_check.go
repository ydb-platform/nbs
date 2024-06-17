package admin

import (
	"encoding/json"
	"log"
	"os"

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

	t := &consistencyCheckTask{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "check",
		RunE: func(cmd *cobra.Command, args []string) error {
			return t.run()
		},
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type baseDisksConsistencyCheckTask struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
}

func (t *baseDisksConsistencyCheckTask) run() error {
	ctx := newContext(t.clientConfig)

	poolsStorage, db, err := newPoolStorage(ctx, t.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	return poolsStorage.CheckBaseDisksConsistency(ctx)
}

func newBaseDisksConsistencyCheckTask(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	t := &baseDisksConsistencyCheckTask{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "check_base_disks",
		RunE: func(cmd *cobra.Command, args []string) error {
			return t.run()
		},
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type poolsConsistencyCheckTask struct {
	clientConfig        *client_config.ClientConfig
	serverConfig        *server_config.ServerConfig
	transitionsFilePath string
}

func (t *poolsConsistencyCheckTask) run() error {
	ctx := newContext(t.clientConfig)

	poolsStorage, db, err := newPoolStorage(ctx, t.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	transitions, err := poolsStorage.CheckPoolsConsistency(ctx)
	if err != nil {
		return err
	}

	if len(t.transitionsFilePath) != 0 {
		f, err := os.OpenFile(
			t.transitionsFilePath,
			os.O_TRUNC|os.O_WRONLY|os.O_CREATE,
			0644,
		)
		if err != nil {
			return err
		}
		defer f.Close()

		s, err := json.Marshal(transitions)
		if err != nil {
			return err
		}

		_, err = f.Write(s)
		if err != nil {
			return err
		}
	}

	return nil
}

func newPoolsConsistencyCheckTask(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	t := &poolsConsistencyCheckTask{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "check_pools",
		RunE: func(cmd *cobra.Command, args []string) error {
			return t.run()
		},
	}

	cmd.Flags().StringVar(&t.transitionsFilePath, "path", "", "file path where offsetting transitions will be saved; optional")
	if err := cmd.MarkFlagFilename("path"); err != nil {
		log.Fatalf("Error setting flag path as required: %v", err)
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
		newBaseDisksConsistencyCheckTask(clientConfig, serverConfig),
		newPoolsConsistencyCheckTask(clientConfig, serverConfig),
	)

	return cmd
}
