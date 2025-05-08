package admin

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/util"
)

////////////////////////////////////////////////////////////////////////////////

func Run(
	use string,
	defaultClientConfigFilePath string,
	defaultServerConfigFilePath string,
) {

	var clientConfigFilePath, serverConfigFilePath string
	clientConfig := &client_config.ClientConfig{}
	serverConfig := &server_config.ServerConfig{}
	idExtracted := false

	rootCmd := &cobra.Command{
		Use:   use,
		Short: "Admin console for Disk Manager service",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// cmd.Name() would be name of the most specific subcommand
			// for "disk-manager-admin completion bash" it would be "bash".
			// So, we need to go up the tree to find the root command's child.
			command := cmd
			for ; command.Parent() != cmd.Root(); command = command.Parent() {
			}

			helperCommands := map[string]struct{}{
				"__complete":       {},
				"__completeNoDesc": {},
				"completion":       {},
			}
			if _, ok := helperCommands[command.Name()]; ok {
				return nil
			}

			err := util.ParseProto(clientConfigFilePath, clientConfig)
			if err != nil {
				return err
			}

			if idExtracted {
				return nil
			}
			idExtracted = true

			flag := cmd.Flags().Lookup("id")
			if flag == nil {
				return nil
			}

			if flag.Changed {
				if len(args) == 0 {
					return nil
				}
				return fmt.Errorf("'--id' can't be passed as both positional and keyword arguments")
			}

			if len(args) < 1 {
				return fmt.Errorf("'--id' must be passed as a positional argument")
			}
			return cmd.Flags().Set("id", args[0])
		},
		CompletionOptions: struct {
			DisableDefaultCmd   bool
			DisableNoDescFlag   bool
			DisableDescriptions bool
			HiddenDefaultCmd    bool
		}{
			DisableDefaultCmd:   false,
			DisableNoDescFlag:   false,
			DisableDescriptions: true,
			HiddenDefaultCmd:    true,
		},
	}

	rootCmd.PersistentFlags().StringVar(
		&clientConfigFilePath,
		"config",
		defaultClientConfigFilePath,
		"Path to the client config file",
	)

	rootCmd.PersistentFlags().StringVar(
		&serverConfigFilePath,
		"server-config",
		defaultServerConfigFilePath,
		"Path to the server config file",
	)

	rootCmd.AddCommand(
		newOperationsCmd(clientConfig),
	)

	commandsWhichRequireServerConfig := []*cobra.Command{
		newPrivateCmd(clientConfig, serverConfig),
		newDisksCmd(clientConfig, serverConfig),
		newTasksCmd(clientConfig, serverConfig),
		newImagesCmd(clientConfig, serverConfig),
		newSnapshotsCmd(clientConfig, serverConfig),
		newFilesystemCmd(clientConfig, serverConfig),
		newPlacementGroupCmd(clientConfig, serverConfig),
		newPoolsCmd(clientConfig, serverConfig),
	}

	parseClientAndServerConfig := func(cmd *cobra.Command, args []string) error {
		err := rootCmd.PersistentPreRunE(cmd, args)
		if err != nil {
			return err
		}

		return util.ParseProto(serverConfigFilePath, serverConfig)
	}

	for _, cmd := range commandsWhichRequireServerConfig {
		cmd.PersistentPreRunE = parseClientAndServerConfig
		rootCmd.AddCommand(cmd)
	}

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
