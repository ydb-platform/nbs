package admin

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
)

////////////////////////////////////////////////////////////////////////////////

type consistencyCheck struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
}

func (t *consistencyCheck) run() error {
	ctx := newContext(t.clientConfig)

	poolStorage, db, err := newPoolStorage(ctx, t.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	return poolStorage.CheckConsistency(ctx)
}

func newConsistencyCheck(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	t := &consistencyCheck{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use:     "check-consistency",
		Aliases: []string{"check_consistency"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return t.run()
		},
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type baseDisksConsistencyCheck struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
}

func (t *baseDisksConsistencyCheck) run() error {
	ctx := newContext(t.clientConfig)

	poolStorage, db, err := newPoolStorage(ctx, t.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	return poolStorage.CheckBaseDisksConsistency(ctx)
}

func newBaseDisksConsistencyCheck(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	t := &baseDisksConsistencyCheck{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use:     "check-base-disks-consistency",
		Aliases: []string{"check_base_disks_consistency"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return t.run()
		},
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type poolsConsistencyCheck struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
}

func (t *poolsConsistencyCheck) run() error {
	ctx := newContext(t.clientConfig)

	poolStorage, db, err := newPoolStorage(ctx, t.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	transitions, err := poolStorage.CheckPoolsConsistency(ctx)
	if err != nil {
		return err
	}

	j, err := json.Marshal(transitions)
	if err != nil {
		return err
	}

	fmt.Println(string(j))

	return nil
}

func newPoolsConsistencyCheck(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	t := &poolsConsistencyCheck{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use:     "check-pools-consistency",
		Aliases: []string{"check_pools_consistency"},
		RunE: func(cmd *cobra.Command, args []string) error {
			return t.run()
		},
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

func newPoolsCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmd := &cobra.Command{
		Use:     "pools",
		Aliases: []string{"pool"},
	}

	cmd.AddCommand(
		newConsistencyCheck(clientConfig, serverConfig),
		newBaseDisksConsistencyCheck(clientConfig, serverConfig),
		newPoolsConsistencyCheck(clientConfig, serverConfig),
	)

	return cmd
}
