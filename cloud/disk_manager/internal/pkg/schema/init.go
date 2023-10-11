package schema

import (
	"context"
	"fmt"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func ParseServerConfig(
	configFilePath string,
	config *server_config.ServerConfig,
) error {

	configBytes, err := os.ReadFile(configFilePath)
	if err != nil {
		return fmt.Errorf(
			"failed to read config file %v: %w",
			configFilePath,
			err,
		)
	}

	err = proto.UnmarshalText(string(configBytes), config)
	if err != nil {
		return fmt.Errorf(
			"failed to parse config file %v as protobuf: %w",
			configFilePath,
			err,
		)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func Init(
	ctx context.Context,
	config *server_config.ServerConfig,
	dropUnusedColumns bool,
) error {

	logging.Info(ctx, "Initiliazing schema")

	creds := auth.NewCredentials(ctx, config.GetAuthConfig())

	db, err := persistence.NewYDBClient(
		ctx,
		config.GetPersistenceConfig(),
		metrics.NewEmptyRegistry(),
		persistence.WithCredentials(creds),
	)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	if config.GetDataplaneConfig() != nil {
		err = initDataplane(ctx, config, creds, db, dropUnusedColumns)
		if err != nil {
			return err
		}
	}

	// TODO: should not use GrpcConfig here.
	if config.GetGrpcConfig() != nil {
		err = initControlplane(ctx, config, db, dropUnusedColumns)
		if err != nil {
			return err
		}
	}

	logging.Info(ctx, "Initialized schema")
	return nil
}
