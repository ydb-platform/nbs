package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	operation_proto "github.com/ydb-platform/nbs/cloud/disk_manager/api/operation"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/headers"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
)

////////////////////////////////////////////////////////////////////////////////

var testSuiteName string = "DiskManagerLoadTests"
var curLaunchID string
var lastReqNumber int

////////////////////////////////////////////////////////////////////////////////

func generateID() string {
	return fmt.Sprintf("%v_%v", testSuiteName, uuid.Must(uuid.NewV4()).String())
}

func getRequestContext(ctx context.Context) context.Context {
	if len(curLaunchID) == 0 {
		curLaunchID = generateID()
	}

	lastReqNumber++

	return headers.SetOutgoingIdempotencyKey(
		ctx,
		fmt.Sprintf("%v_%v", curLaunchID, lastReqNumber),
	)
}

////////////////////////////////////////////////////////////////////////////////

func parseConfig(
	configFileName string,
	config *client_config.ClientConfig,
) error {

	log.Printf("Reading DM client config file=%v", configFileName)

	configBytes, err := os.ReadFile(configFileName)
	if err != nil {
		return fmt.Errorf(
			"failed to read Disk Manager config file %v: %w",
			configFileName,
			err,
		)
	}

	log.Printf("Parsing DM client config file as protobuf")

	err = proto.UnmarshalText(string(configBytes), config)
	if err != nil {
		return fmt.Errorf(
			"failed to parse Disk Manager config file %v as protobuf: %w",
			configFileName,
			err,
		)
	}

	return nil
}

func waitOperation(
	ctx context.Context,
	client internal_client.PrivateClient,
	operation *operation_proto.Operation,
) error {

	var err error
	operation, err = client.WaitOperation(
		ctx,
		operation.Id,
		func(context.Context, *operation_proto.Operation) error { return nil },
	)
	if err != nil {
		log.Printf("Wait operation %v failed: %v", operation, err)
		return fmt.Errorf("wait operation %v failed: %w", operation, err)
	}

	return nil
}

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.InfoLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

type Scheduled struct {
	operation *operation_proto.Operation
	err       error
}

////////////////////////////////////////////////////////////////////////////////

func runTest(
	config *client_config.ClientConfig,
	depth uint32,
	durationSec uint32,
) error {

	curLaunchID = generateID()

	ctx := newContext()

	log.Printf("Creating DM client with config=%v", config)

	client, err := internal_client.NewPrivateClientForCLI(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	log.Printf("Starting test")

	start := time.Now()
	count := 0

	for {
		operations := make([]*operation_proto.Operation, 0, depth)
		scheduled := make(chan Scheduled)

		for i := uint32(0); i < depth; i++ {
			go func() {
				operation, err := client.ScheduleBlankOperation(
					getRequestContext(ctx),
				)

				scheduled <- Scheduled{
					operation: operation,
					err:       err,
				}
			}()
		}

		for i := uint32(0); i < depth; i++ {
			s := <-scheduled
			if s.err != nil {
				log.Printf("Schedule blank operation failed: %v", s.err)
				return s.err
			}

			operations = append(operations, s.operation)
		}

		log.Printf("%v operations scheduled", len(operations))

		for _, operation := range operations {
			err = waitOperation(ctx, client, operation)
			if err != nil {
				return err
			}

			count++
		}

		log.Printf("%v operations done, elapsed=%v", count, time.Since(start))

		if time.Since(start).Seconds() >= float64(durationSec) {
			break
		}
	}

	log.Printf(
		"Test successfully finished. %v operations done, elapsed=%v",
		count,
		time.Since(start),
	)
	return nil
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	var configFileName string
	config := &client_config.ClientConfig{}

	var depth uint32
	var durationSec uint32

	rootCmd := &cobra.Command{
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return parseConfig(configFileName, config)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTest(config, depth, durationSec)
		},
	}

	rootCmd.PersistentFlags().StringVar(
		&configFileName,
		"disk-manager-client-config",
		"/etc/yc/disk-manager/client-config.txt",
		"Path to the Disk Manager client config file",
	)

	rootCmd.Flags().Uint32Var(&depth, "depth", 0, "number of tasks inflight")
	if err := rootCmd.MarkFlagRequired("depth"); err != nil {
		log.Fatalf("Error setting flag depth as required: %v", err)
	}

	rootCmd.Flags().Uint32Var(&durationSec, "duration", 0, "lower bound on test duration (in seconds)")
	if err := rootCmd.MarkFlagRequired("duration"); err != nil {
		log.Fatalf("Error setting flag duration as required: %v", err)
	}

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Failed to run: %v", err)
	}
}
