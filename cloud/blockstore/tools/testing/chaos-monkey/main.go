package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/spf13/cobra"

	nbs "a.yandex-team.ru/cloud/blockstore/public/sdk/go/client"
)

////////////////////////////////////////////////////////////////////////////////

type options struct {
	DiskRegistryStateFile    string
	NbsHost                  string
	NbsPort                  int
	NbsToken                 string
	MaxTargets               int
	FolderIDs                []string
	Apply                    bool
	WaitForReplicationStart  bool
	WaitForReplicationFinish bool
	Cleanup                  bool
}

////////////////////////////////////////////////////////////////////////////////

func run(ctx context.Context, opts *options) error {
	var stateJSON []byte
	var err error

	var client *nbs.Client
	if len(opts.NbsHost) > 0 {
		client, err = makeClient(opts.NbsHost, opts.NbsPort, opts.NbsToken)

		if err != nil {
			return fmt.Errorf("can't create nbs client: %v", err)
		}
	}

	if len(opts.DiskRegistryStateFile) > 0 {
		stateJSON, err = ioutil.ReadFile(opts.DiskRegistryStateFile)
		if err != nil {
			return fmt.Errorf(
				"can't read state file %v: %w",
				opts.DiskRegistryStateFile,
				err)
		}
	} else {
		stateJSON, err = backupDiskRegistryState(ctx, client)

		if err != nil {
			return fmt.Errorf(
				"can't fetch state from nbs: %w", err)
		}
	}

	var state *DiskRegistryStateBackup
	if len(stateJSON) != 0 {
		err = json.Unmarshal(stateJSON, &state)
		if err != nil {
			return fmt.Errorf(
				"can't unmarshal state from %v: %w",
				opts.DiskRegistryStateFile,
				err)
		}
	}

	if opts.Cleanup {
		outputs, err := cleanup(ctx, state, client)

		if err != nil {
			return err
		}

		for _, o := range outputs {
			fmt.Printf("cleanup: %v\n", o)
		}

		return nil
	}

	targets, alternativeTargets, err := findTestTargets(state, opts.FolderIDs)

	if err != nil {
		return err
	}

	output, err := json.MarshalIndent(targets, "", "    ")
	if err != nil {
		return err
	}

	fmt.Println("targets:")
	fmt.Println(string(output))

	output, err = json.MarshalIndent(alternativeTargets, "", "    ")
	if err != nil {
		return err
	}

	fmt.Println("alternative targets:")
	fmt.Println(string(output))

	if len(targets) > opts.MaxTargets {
		targets = targets[:opts.MaxTargets]
	}

	if opts.Apply {
		outputs, err := applyTargets(ctx, targets, client)

		if err != nil {
			return err
		}

		for _, o := range outputs {
			fmt.Printf("apply: %v\n", o)
		}
	}

	if opts.WaitForReplicationStart {
		outputs, err := waitForReplicationStart(ctx, targets, client)

		if err != nil {
			return err
		}

		for _, o := range outputs {
			fmt.Printf("wait for replication start: %v\n", o)
		}
	}

	if opts.WaitForReplicationFinish {
		outputs, err := waitForReplicationFinish(ctx, targets, client)

		if err != nil {
			return err
		}

		for _, o := range outputs {
			fmt.Printf("wait for replication finish: %v\n", o)
		}
	}

	return nil
}

func main() {
	var opts options

	var rootCmd = &cobra.Command{
		Use:   "blockstore-chaos-monkey",
		Short: "Chaos Monkey tool",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancelCtx := context.WithCancel(context.Background())
			defer cancelCtx()

			if err := run(ctx, &opts); err != nil {
				log.Fatalf("Error: %v", err)
			}
		},
	}

	/*
		markFlagRequired := func(flag string) {
			if err := rootCmd.MarkFlagRequired(flag); err != nil {
				log.Fatalf("can't mark flag %v as required: %v", flag, err)
			}
		}
	*/

	rootCmd.Flags().StringVar(
		&opts.DiskRegistryStateFile,
		"dr-state",
		"",
		"path to DiskRegistry state file",
	)

	rootCmd.Flags().StringVar(
		&opts.NbsHost,
		"nbs-host",
		"localhost",
		"nbs host",
	)

	rootCmd.Flags().IntVar(
		&opts.NbsPort,
		"nbs-port",
		9766,
		"nbs port",
	)

	rootCmd.Flags().IntVar(
		&opts.MaxTargets,
		"max-targets",
		1,
		"max targets to process",
	)

	rootCmd.Flags().BoolVar(
		&opts.Apply,
		"apply",
		false,
		"applies state analysis results (sends DisableAgent requests)",
	)

	rootCmd.Flags().BoolVar(
		&opts.WaitForReplicationStart,
		"wait-for-replication-start",
		false,
		"waits until replication starts for the affected disks",
	)

	rootCmd.Flags().BoolVar(
		&opts.WaitForReplicationFinish,
		"wait-for-replication-finish",
		false,
		"waits until replication finishes for the affected disks",
	)

	rootCmd.Flags().BoolVar(
		&opts.Cleanup,
		"cleanup",
		false,
		"restores state to DEVICE_STATE_ONLINE for the replaced devices",
	)

	opts.NbsToken, _ = os.LookupEnv("NBS_TOKEN")

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("can't execute root command: %v", err)
	}
}
