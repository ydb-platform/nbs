package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	nbs "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
)

////////////////////////////////////////////////////////////////////////////////

type options struct {
	DiskRegistryStateFile     string
	DumpDiskRegistryStateFile string
	NbsHost                   string
	NbsPort                   int
	NbsToken                  string
	DiskID                    string
	DeviceID                  string
	MaxTargets                int
	CanBreakTwoDevicesInCell  bool
	PreferFreshDevices        bool
	PreferMinusOneCells       bool
	FolderIDs                 []string
	Apply                     bool
	WaitForReplicationStart   bool
	WaitForReplicationFinish  bool
	Cleanup                   bool
	Heal                      bool
	LogDateTime               bool
}

////////////////////////////////////////////////////////////////////////////////

func run(ctx context.Context, opts *options) error {
	if opts.LogDateTime {
		log.SetFlags(log.Ldate | log.Ltime)
	}

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
		stateJSON, err = os.ReadFile(opts.DiskRegistryStateFile)
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

	if len(opts.DumpDiskRegistryStateFile) > 0 {
		err = os.WriteFile(opts.DumpDiskRegistryStateFile, stateJSON, 0644)
		if err != nil {
			return fmt.Errorf(
				"can't save disk registry dump file: %w", err)
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
			log.Printf("cleanup: %v", o)
		}

		return nil
	}

	if len(opts.DiskID) == 0 {
		return fmt.Errorf("need DiskId")
	}

	var diskInfo *DiskInfo
	log.Printf("Requesting DiskInfo...")
	diskInfo, err = describeDisk(state, opts.DiskID)
	if err != nil {
		return err
	}
	output, err := json.MarshalIndent(*diskInfo, "", "    ")
	if err != nil {
		return err
	}
	log.Printf("DiskInfo:\n%v", string(output))

	log.Printf("Detecting targets to curse...")
	targetsToCurse := findAllTargetsToCurse(diskInfo, opts.MaxTargets,
		opts.CanBreakTwoDevicesInCell,
		opts.PreferFreshDevices,
		opts.PreferMinusOneCells,
		opts.DeviceID)
	output, err = json.MarshalIndent(targetsToCurse, "", "    ")
	if err != nil {
		return err
	}
	log.Printf("Targets to curse:\n%v", string(output))

	log.Printf("Detecting targets to heal...")
	targetsToHeal := findTargetsToHeal(diskInfo, opts.MaxTargets, opts.DeviceID)
	output, err = json.MarshalIndent(targetsToHeal, "", "    ")
	if err != nil {
		return err
	}
	log.Printf("Targets to heal:\n%v", string(output))

	if opts.Heal {
		outputs, err := heal(ctx, targetsToHeal, client)
		if err != nil {
			return err
		}

		for _, o := range outputs {
			log.Printf("cleanup: %v", o)
		}

		return nil
	}

	if opts.Apply {
		outputs, err := applyTargets(ctx, targetsToCurse, client)

		if err != nil {
			return err
		}

		for _, o := range outputs {
			log.Printf("apply: %v", o)
		}
	}

	if opts.WaitForReplicationStart {
		outputs, err := waitForReplicationStart(ctx, targetsToCurse, client)

		if err != nil {
			return err
		}

		for _, o := range outputs {
			log.Printf("Replication started: %v", o)
		}
	}

	if opts.WaitForReplicationFinish {
		outputs, err := waitForReplicationFinish(ctx, targetsToCurse, client)

		if err != nil {
			return err
		}

		for _, o := range outputs {
			log.Printf("Replication finished: %v", o)
		}
	}

	return nil
}

func main() {
	var opts options
	log.SetFlags(0)
	log.SetOutput(os.Stdout)

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

	rootCmd.Flags().StringVar(
		&opts.DiskRegistryStateFile,
		"dr-state",
		"",
		"path to DiskRegistry state file",
	)

	rootCmd.Flags().StringVar(
		&opts.DumpDiskRegistryStateFile,
		"dump-dr-state",
		"",
		"path to DiskRegistry state file to dump",
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

	rootCmd.Flags().StringVar(
		&opts.DiskID,
		"disk-id",
		"",
		"disk id",
	)

	rootCmd.Flags().StringVar(
		&opts.DeviceID,
		"device-id",
		"",
		"Explicit device ID to be cursed or healed",
	)

	rootCmd.Flags().IntVar(
		&opts.MaxTargets,
		"max-targets",
		1,
		"max targets to process",
	)

	rootCmd.Flags().BoolVar(
		&opts.CanBreakTwoDevicesInCell,
		"can-break-two",
		false,
		"allows the monkey to break two devices in the cell",
	)

	rootCmd.Flags().BoolVar(
		&opts.PreferFreshDevices,
		"prefer-fresh",
		false,
		"tell the monkey to break fresh devices",
	)

	rootCmd.Flags().BoolVar(
		&opts.PreferMinusOneCells,
		"prefer-minus-one",
		false,
		"tell the monkey to break devices in the cells with minus one",
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
		"restores state to DEVICE_STATE_ONLINE for all replaced devices",
	)

	rootCmd.Flags().BoolVar(
		&opts.Heal,
		"heal",
		false,
		"restores state to DEVICE_STATE_ONLINE for the replaced devices for disk",
	)

	rootCmd.Flags().BoolVar(
		&opts.LogDateTime,
		"log-datetime",
		false,
		"adds date and time to log output",
	)

	opts.NbsToken, _ = os.LookupEnv("NBS_TOKEN")

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("can't execute root command: %v", err)
	}
}
