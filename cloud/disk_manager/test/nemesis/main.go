package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.InfoLevel),
	)
}

func runIteration(ctx context.Context, cmdString string) error {
	logging.Info(ctx, "Running command: %v", cmdString)

	split := strings.Split(cmdString, " ")
	cmd := exec.CommandContext(ctx, split[0], split[1:]...)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		return err
	}

	pid := cmd.Process.Pid

	logging.Info(ctx, "Waiting for process with PID: %v", pid)
	return cmd.Wait()
}

func waitIteration(
	ctx context.Context,
	cancel func(),
	errors chan error,
	minRestartPeriodSec uint32,
	maxRestartPeriodSec uint32,
) error {

	restartPeriod := common.RandomDuration(
		time.Duration(minRestartPeriodSec)*time.Second,
		time.Duration(maxRestartPeriodSec)*time.Second,
	)

	select {
	case <-time.After(restartPeriod):
		logging.Info(ctx, "Cancel iteration")
		cancel()
		// Wait for iteration to stop.
		<-errors
		return nil
	case err := <-errors:
		logging.Error(ctx, "Received error during iteration: %v", err)
		return err
	}
}

func run(
	cmdString string,
	minRestartPeriodSec uint32,
	maxRestartPeriodSec uint32,
) error {

	cmdString = strings.TrimSpace(cmdString)
	if len(cmdString) == 0 {
		return fmt.Errorf("invalid command: %v", cmdString)
	}

	ctx := newContext()

	for {
		logging.Info(ctx, "Start iteration")

		iterationCtx, cancelIteration := context.WithCancel(ctx)
		defer cancelIteration()

		errors := make(chan error, 1)
		go func() {
			errors <- runIteration(iterationCtx, cmdString)
		}()

		err := waitIteration(
			iterationCtx,
			cancelIteration,
			errors,
			minRestartPeriodSec,
			maxRestartPeriodSec,
		)
		if err != nil {
			return err
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	var cmdString string
	var minRestartPeriodSec uint32
	var maxRestartPeriodSec uint32

	rootCmd := &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmdString, minRestartPeriodSec, maxRestartPeriodSec)
		},
	}

	rootCmd.Flags().StringVar(&cmdString, "cmd", "", "command to execute")
	if err := rootCmd.MarkFlagRequired("cmd"); err != nil {
		log.Fatalf("Error setting flag cmd as required: %v", err)
	}

	rootCmd.Flags().Uint32Var(
		&minRestartPeriodSec,
		"min-restart-period-sec",
		5,
		"minimum time (in seconds) between two consecutive restarts",
	)

	rootCmd.Flags().Uint32Var(
		&maxRestartPeriodSec,
		"max-restart-period-sec",
		30,
		"maximum time (in seconds) between two consecutive restarts",
	)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Failed to execute: %v", err)
	}
}
