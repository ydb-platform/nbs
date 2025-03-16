package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/cloud/tasks"
	node_config "github.com/ydb-platform/nbs/cloud/tasks/acceptance_tests/recipe/node/config"
	recipe_tasks "github.com/ydb-platform/nbs/cloud/tasks/acceptance_tests/recipe/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
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
		empty.NewRegistry(),
	)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	storage, err := tasks_storage.NewStorage(
		config.GetTasksConfig(),
		empty.NewRegistry(),
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
		empty.NewRegistry(),
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

	availabilityMonitoringStorage := persistence.NewAvailabilityMonitoringStorage(
		config.GetTasksConfig(),
		db,
		10, // banInterval
		10, // maxBanHostsCount
	)

	err = tasks.StartRunners(
		ctx,
		storage,
		availabilityMonitoringStorage,
		registry,
		empty.NewRegistry(),
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
