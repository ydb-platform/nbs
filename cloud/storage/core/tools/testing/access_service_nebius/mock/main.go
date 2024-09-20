package main

import (
	"log"

	"github.com/spf13/cobra"

	"github.com/ydb-platform/nbs/cloud/storage/core/tools/testing/access_service_nebius/mock/access_service"
)

////////////////////////////////////////////////////////////////////////////////

func main() {
	var configPath string
	rootCmd := &cobra.Command{
		Use: "Access Service Mock",
		RunE: func(cmd *cobra.Command, args []string) error {
			return access_service.StartAccessService(configPath)
		},
	}

	rootCmd.Flags().StringVar(&configPath, "config-path", "", "Path to a config")
	if err := rootCmd.MarkFlagRequired("config-path"); err != nil {
		log.Fatalf("Config path is required: %v", err)
	}

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error while starting server: %v", err)
	}
}
