package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/persistence"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks"
	node_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/acceptance_tests/recipe/node/config"
	recipe_tasks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/acceptance_tests/recipe/tasks"
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

func run(config *node_config.Config) error {
	logger := logging.NewLogger(config.GetLoggingConfig())
	ctx := logging.SetLogger(context.Background(), logger)

	db, err := persistence.NewYDBClient(
		ctx,
		config.GetPersistenceConfig(),
		metrics.NewEmptyRegistry(),
	)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	storage, err := tasks_storage.NewStorage(
		config.GetTasksConfig(),
		metrics.NewEmptyRegistry(),
		db,
	)
	if err != nil {
		return err
	}

	registry := tasks.NewRegistry()

	scheduler, err := tasks.NewScheduler(
		ctx,
		registry,
		storage,
		config.GetTasksConfig(),
		metrics.NewEmptyRegistry(),
	)
	if err != nil {
		return err
	}

	err = registry.RegisterForExecution("ChainTask", func() tasks.Task {
		return recipe_tasks.NewChainTask(scheduler)
	})
	if err != nil {
		return err
	}

	err = registry.RegisterForExecution("PanicTask", func() tasks.Task {
		return recipe_tasks.NewPanicTask()
	})
	if err != nil {
		return err
	}

	err = tasks.StartRunners(
		ctx,
		storage,
		registry,
		metrics.NewEmptyRegistry(),
		config.GetTasksConfig(),
		config.GetHostname(),
	)
	if err != nil {
		return err
	}

	select {}
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	var configFileName string
	config := &node_config.Config{}

	rootCmd := &cobra.Command{
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
		log.Fatalf("Failed to run: %v", err)
	}
}
