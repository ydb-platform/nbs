package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	node_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/acceptance_tests/recipe/node/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/persistence"
	tasks_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

func parseConfig(
	configFileName string,
	config *node_config.Config,
) error {

	configBytes, err := os.ReadFile(configFileName)
	if err != nil {
		return fmt.Errorf(
			"failed to read config file %v: %w",
			configFileName,
			err,
		)
	}

	err = proto.UnmarshalText(string(configBytes), config)
	if err != nil {
		return fmt.Errorf(
			"failed to parse config file %v as protobuf: %w",
			configFileName,
			err,
		)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func run(
	config *node_config.Config,
) error {

	ctx := context.Background()

	logger := logging.NewStderrLogger(logging.DebugLevel)
	ctx = logging.SetLogger(ctx, logger)

	logging.Info(ctx, "Node init DB running")

	db, err := persistence.NewYDBClient(
		ctx,
		config.GetPersistenceConfig(),
		metrics.NewEmptyRegistry(),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer db.Close(ctx)

	err = tasks_storage.CreateYDBTables(
		ctx,
		config.GetTasksConfig(),
		db,
		false, // dropUnusedColumns
	)
	if err != nil {
		return fmt.Errorf("failed to create tables for tasks: %w", err)
	}

	logging.Info(ctx, "Node init DB done")

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	var configFileName string
	config := &node_config.Config{}

	var rootCmd = &cobra.Command{
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return parseConfig(configFileName, config)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(config)
		},
	}
	rootCmd.Flags().StringVar(
		&configFileName,
		"config",
		"",
		"Path to the config file",
	)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
