package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/cloud/tasks/common"
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

// Controlled mode consumes a monotonically increasing generation from a
// recipe-owned file. The test must replace the file atomically when increasing
// it. Reading the initial generation before spawning the first child prevents
// a request from being lost between child startup and watcher initialization.
type restartTrigger struct {
	path string
	last uint64
}

func readRestartGeneration(path string) (uint64, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() {
		return 0, fmt.Errorf("restart trigger must be a regular file: %s", path)
	}
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 33))
	if err != nil {
		return 0, err
	}
	if len(data) > 32 {
		return 0, fmt.Errorf("restart trigger counter is too long")
	}
	return strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
}

func newRestartTrigger(path string) (*restartTrigger, error) {
	if path == "" {
		return nil, nil
	}
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("restart trigger path must be absolute")
	}
	value, err := readRestartGeneration(path)
	if err != nil {
		return nil, err
	}
	return &restartTrigger{path: path, last: value}, nil
}

func (t *restartTrigger) requested() (bool, error) {
	value, err := readRestartGeneration(t.path)
	if err != nil {
		return false, err
	}
	if value < t.last {
		return false, fmt.Errorf("restart trigger counter decreased from %d to %d", t.last, value)
	}
	if value == t.last {
		return false, nil
	}
	t.last = value
	return true, nil
}

func recordRestart(restartTimingsFile string) error {
	if restartTimingsFile == "" {
		return nil
	}
	file, err := os.OpenFile(restartTimingsFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(time.Now().String())
	return err
}

func waitIteration(
	ctx context.Context,
	cancel func(),
	errors chan error,
	minRestartPeriodSec uint32,
	maxRestartPeriodSec uint32,
	restartTimingsFile string,
	trigger *restartTrigger,
) error {

	var wakeup <-chan time.Time
	if trigger == nil {
		restartPeriod := common.RandomDuration(
			time.Duration(minRestartPeriodSec)*time.Second,
			time.Duration(maxRestartPeriodSec)*time.Second,
		)
		timer := time.NewTimer(restartPeriod)
		defer timer.Stop()
		wakeup = timer.C
	} else {
		// No random restart timer exists in controlled mode.
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		wakeup = ticker.C
	}
	for {
		select {
		case <-ctx.Done():
			cancel()
			<-errors
			return ctx.Err()
		case <-wakeup:
			if trigger != nil {
				requested, err := trigger.requested()
				if err != nil {
					cancel()
					<-errors
					return err
				}
				if !requested {
					continue
				}
			}
			logging.Info(ctx, "Cancel iteration")
			cancel()
			// Do not start a replacement or acknowledge a restart until the
			// old child was actually reaped by cmd.Wait.
			<-errors
			return recordRestart(restartTimingsFile)
		case err := <-errors:
			logging.Error(ctx, "Received error during iteration: %v", err)
			if trigger != nil && err == nil {
				return fmt.Errorf("controlled child exited without a restart request")
			}
			return err
		}
	}
}

func run(
	cmdString string,
	minRestartPeriodSec uint32,
	maxRestartPeriodSec uint32,
	restartTimingsFile string,
	restartTriggerFile string,
) error {

	cmdString = strings.TrimSpace(cmdString)
	if len(cmdString) == 0 {
		return fmt.Errorf("invalid command: %v", cmdString)
	}

	ctx := newContext()
	trigger, err := newRestartTrigger(restartTriggerFile)
	if err != nil {
		return fmt.Errorf("invalid restart trigger: %w", err)
	}

	for {
		logging.Info(ctx, "Start iteration")

		iterationCtx, cancelIteration := context.WithCancel(ctx)

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
			restartTimingsFile,
			trigger,
		)
		cancelIteration()
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
	var restartTimingsFile string
	var restartTriggerFile string

	rootCmd := &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(
				cmdString,
				minRestartPeriodSec,
				maxRestartPeriodSec,
				restartTimingsFile,
				restartTriggerFile,
			)
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

	rootCmd.Flags().StringVar(
		&restartTriggerFile,
		"restart-trigger-file",
		"",
		"disable random restarts; restart when this existing absolute counter file increases",
	)

	rootCmd.Flags().StringVar(
		&restartTimingsFile,
		"restart-timings-file",
		"",
		"file where to store restart timings",
	)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Failed to execute: %v", err)
	}
}
