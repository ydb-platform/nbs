package admin

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gofrs/uuid"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	pools_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
	"google.golang.org/grpc/metadata"
)

////////////////////////////////////////////////////////////////////////////////

var curLaunchID string
var lastReqNumber int

////////////////////////////////////////////////////////////////////////////////

func addAuthHeader(ctx context.Context) context.Context {
	token := os.Getenv("IAM_TOKEN")
	return metadata.NewOutgoingContext(
		ctx,
		metadata.Pairs("authorization", fmt.Sprintf("Bearer %v", token)),
	)
}

func newContext(config *client_config.ClientConfig) context.Context {
	ctx := context.Background()
	logger := logging.NewLogger(config.LoggingConfig)
	ctx = logging.SetLogger(ctx, logger)
	return addAuthHeader(ctx)
}

////////////////////////////////////////////////////////////////////////////////

func generateID() string {
	return fmt.Sprintf(
		"%v_%v",
		"disk-manager-admin",
		uuid.Must(uuid.NewV4()).String(),
	)
}

func getRequestContext(ctx context.Context) context.Context {
	if len(curLaunchID) == 0 {
		curLaunchID = generateID()
	}

	lastReqNumber++

	cookie := fmt.Sprintf("%v_%v", curLaunchID, lastReqNumber)
	ctx = headers.SetOutgoingIdempotencyKey(ctx, cookie)
	ctx = headers.SetOutgoingRequestID(ctx, cookie)
	return ctx
}

func requestConfirmation(objectType string, objectID string) error {
	log.Printf("confirm command by typing %s id to stdin", objectType)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	if scanner.Text() != objectID {
		return fmt.Errorf(
			"confirmation failed: %s != %s",
			scanner.Text(),
			objectID)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func newYDBClient(
	ctx context.Context,
	config *server_config.ServerConfig,
) (*persistence.YDBClient, error) {

	creds := auth.NewCredentials(ctx, config.AuthConfig)

	db, err := persistence.NewYDBClient(
		ctx,
		config.PersistenceConfig,
		metrics.NewEmptyRegistry(),
		persistence.WithCredentials(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DB: %w", err)
	}

	return db, nil
}

func newTaskStorage(
	ctx context.Context,
	config *server_config.ServerConfig,
) (tasks_storage.Storage, *persistence.YDBClient, error) {

	db, err := newYDBClient(ctx, config)
	if err != nil {
		return nil, nil, err
	}

	taskStorage, err := tasks_storage.NewStorage(
		config.TasksConfig,
		metrics.NewEmptyRegistry(),
		db,
	)

	return taskStorage, db, err
}

func newResourceStorage(
	ctx context.Context,
	config *server_config.ServerConfig,
) (resources.Storage, *persistence.YDBClient, error) {

	db, err := newYDBClient(ctx, config)
	if err != nil {
		return nil, nil, err
	}

	endedMigrationExpirationTimeout, err := time.ParseDuration(
		config.GetDisksConfig().GetEndedMigrationExpirationTimeout(),
	)
	if err != nil {
		return nil, nil, err
	}

	resourcesStorage, err := resources.NewStorage(
		config.GetDisksConfig().GetStorageFolder(),
		config.GetImagesConfig().GetStorageFolder(),
		config.GetSnapshotsConfig().GetStorageFolder(),
		config.GetFilesystemConfig().GetStorageFolder(),
		config.GetPlacementGroupConfig().GetStorageFolder(),
		db,
		endedMigrationExpirationTimeout,
	)

	return resourcesStorage, db, err
}

func newPoolStorage(
	ctx context.Context,
	config *server_config.ServerConfig,
) (pools_storage.Storage, *persistence.YDBClient, error) {

	db, err := newYDBClient(ctx, config)
	if err != nil {
		return nil, nil, err
	}

	poolsStorage, err := pools_storage.NewStorage(
		config.GetPoolsConfig(),
		db,
	)

	return poolsStorage, db, err
}
