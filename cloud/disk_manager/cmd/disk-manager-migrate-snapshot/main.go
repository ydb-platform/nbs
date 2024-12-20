package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/util"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"os"
	"os/signal"
	"strings"
	"sync"
)

const chunkSize = 4 * 1024 * 1024

func createStorage(ctx context.Context, filePath string) (storage.Storage, *persistence.YDBClient, error) {
	var snapshotConfig config.SnapshotConfig

	var creds auth.Credentials
	err := util.ParseProto(filePath, &snapshotConfig)
	if err != nil {
		return nil, nil, err
	}

	if !snapshotConfig.PersistenceConfig.GetDisableAuthentication() {
		creds = credentials.NewAccessTokenCredentials(os.Getenv("IAM_TOKEN"))
	}
	snapshotDB, err := persistence.NewYDBClient(
		ctx,
		snapshotConfig.GetPersistenceConfig(),
		metrics.NewEmptyRegistry(),
		persistence.WithCredentials(creds),
	)
	if err != nil {
		return nil, nil, err
	}

	snapshotStorage, err := storage.NewStorage(
		&snapshotConfig,
		metrics.NewEmptyRegistry(),
		snapshotDB,
		nil,
	)
	if err != nil {
		return nil, nil, err
	}

	return snapshotStorage, snapshotDB, nil
}

func obtainUniqueId(
	ctx context.Context,
	database *persistence.YDBClient,
	snapshotId string,
) (string, error) {

	res, err := database.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;

		select chunk_id from chunk_map
		where snapshot_id = $snapshot_id limit 1;
	`, database.AbsolutePath("snapshot")),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotId)),
	)
	if err != nil {
		return "", err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return "", errors.NewNonRetriableErrorf("chunk not found")
	}

	var chunkId string
	err = res.ScanNamed(
		persistence.OptionalWithDefault("chunk_id", &chunkId),
	)
	if err != nil {
		return "", err
	}

	components := strings.Split(chunkId, ".")
	if len(components) != 3 {
		return "", errors.NewNonRetriableErrorf("Unexpected chunk id %s", chunkId)
	}

	return components[0], nil
}

type fileMilestone struct {
	path string
	mu   *sync.Mutex
}

func newFileMilestone(path string) *fileMilestone {
	return &fileMilestone{
		path: path,
		mu:   &sync.Mutex{},
	}
}

func (f *fileMilestone) load() uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.path)
	if err != nil {
		return 0
	}

	return binary.BigEndian.Uint32(data)
}

func (f *fileMilestone) save(value uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer, value)
	return os.WriteFile(f.path, buffer, 0o644)
}

func migrateSnapshot(
	ctx context.Context,
	sourceSnapshotId string,
	sourceConfigPath string,
	destinationConfigPath string,
) error {
	sourceStorage, ydbClient, err := createStorage(ctx, sourceConfigPath)
	if err != nil {
		return err
	}

	destinationStorage, _, err := createStorage(ctx, destinationConfigPath)
	if err != nil {
		return err
	}

	uniqueId, err := obtainUniqueId(ctx, ydbClient, sourceSnapshotId)
	if err != nil {
		return err
	}

	source := snapshot.NewSnapshotSource(sourceSnapshotId, sourceStorage)
	target := snapshot.NewSnapshotTarget(uniqueId, sourceSnapshotId, destinationStorage, true, false)
	transferer := common.Transferer{
		ReaderCount:         1000,
		WriterCount:         1000,
		ChunksInflightLimit: 1000,
		ChunkSize:           chunkSize,
	}
	milestone := newFileMilestone(sourceSnapshotId)
	_, err = transferer.Transfer(
		ctx,
		source,
		target,
		common.Milestone{ChunkIndex: milestone.load()},
		func(ctx context.Context, m common.Milestone) error {
			logging.Info(ctx, "Processed chunk with index %d", m.ChunkIndex)
			milestone.save(m.ChunkIndex)
			return nil
		},
	)

	return err
}

func main() {
	ctx := context.Background()
	ctx = logging.SetLogger(ctx, logging.NewStderrLogger(logging.DebugLevel))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		logging.Info(ctx, "Interrupt signal received, cancelling operations...")
		cancel()
	}()

	var snapshotId string

	var rootCmd = &cobra.Command{
		Use:   "disk-manager-migrate-snapshot",
		Short: "Migrate snapshot to another database",
		RunE: func(cmd *cobra.Command, args []string) error {
			return migrateSnapshot(ctx, snapshotId, "source.txt", "destination.txt")
		},
	}

	rootCmd.Flags().StringVarP(&snapshotId, "snapshot-id", "s", "", "The snapshot ID (required)")
	rootCmd.MarkFlagRequired("snapshot-id")
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
