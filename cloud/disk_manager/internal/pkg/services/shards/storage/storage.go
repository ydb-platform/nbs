package storage

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/common"
	shards_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/shards/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type ClusterCapacity struct {
	ZoneId     string
	ShardId    string
	Kind       types.DiskKind
	TotalBytes uint64
	FreeBytes  uint64
}

////////////////////////////////////////////////////////////////////////////////

type clusterCapacityState struct {
	ZoneId     string
	ShardId    string
	Kind       string
	TotalBytes uint64
	FreeBytes  uint64
	CreatedAt  time.Time
}

////////////////////////////////////////////////////////////////////////////////

func (c *clusterCapacityState) toClusterCapacity() (ClusterCapacity, error) {
	kind, err := common.DiskKindFromString(c.Kind)
	if err != nil {
		return ClusterCapacity{}, err
	}

	return ClusterCapacity{
		ZoneId:     c.ZoneId,
		ShardId:    c.ShardId,
		Kind:       kind,
		TotalBytes: c.TotalBytes,
		FreeBytes:  c.FreeBytes,
	}, nil
}

// Returns ydb entity of the node object.
func (c *clusterCapacityState) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("zone_id", persistence.UTF8Value(c.ZoneId)),
		persistence.StructFieldValue("shard_id", persistence.UTF8Value(c.ShardId)),
		persistence.StructFieldValue("kind", persistence.UTF8Value(c.Kind)),
		persistence.StructFieldValue("total", persistence.Uint64Value(c.TotalBytes)),
		persistence.StructFieldValue("free", persistence.Uint64Value(c.FreeBytes)),
		persistence.StructFieldValue("created_at", persistence.TimestampValue(c.CreatedAt)),
	)
}

////////////////////////////////////////////////////////////////////////////////

func clusterCapacityStateStructTypeString() string {
	return `Struct<
		zone_id: Utf8,
		shard_id: Utf8,
		kind: Utf8,
		total: Uint64,
		free: Uint64,
		created_at: Timestamp>`
}

func clusterCapacityStateTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("shard_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("kind", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("total", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("free", persistence.Optional(persistence.TypeUint64)),

		persistence.WithColumn("created_at", persistence.Optional(persistence.TypeTimestamp)),

		persistence.WithPrimaryKeyColumn("created_at", "shard_id"),
	)
}

func scanClusterCapacity(
	res persistence.Result,
) (capacity clusterCapacityState, err error) {

	err = res.ScanNamed(
		persistence.OptionalWithDefault("zone_id", &capacity.ZoneId),
		persistence.OptionalWithDefault("shard_id", &capacity.ShardId),
		persistence.OptionalWithDefault("kind", &capacity.Kind),
		persistence.OptionalWithDefault("total", &capacity.TotalBytes),
		persistence.OptionalWithDefault("free", &capacity.FreeBytes),
	)

	return capacity, err
}

func scanClusterCapacities(
	ctx context.Context,
	res persistence.Result,
) ([]ClusterCapacity, error) {

	var clusterCapacities []ClusterCapacity

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			capacityState, err := scanClusterCapacity(res)
			if err != nil {
				return nil, err
			}

			capacity, err := capacityState.toClusterCapacity()
			if err != nil {
				return nil, err
			}

			clusterCapacities = append(clusterCapacities, capacity)
		}
	}

	return clusterCapacities, nil
}

////////////////////////////////////////////////////////////////////////////////

type Storage interface {
	AddClusterCapacities(
		ctx context.Context,
		capacities []ClusterCapacity,
	) error

	GetClusterCapacities(
		ctx context.Context,
		zone_id string,
		kind types.DiskKind,
	) ([]ClusterCapacity, error)
}

////////////////////////////////////////////////////////////////////////////////

func CreateYDBTables(
	ctx context.Context,
	config *shards_config.ShardsConfig,
	db *persistence.YDBClient,
	dropUnusedColumns bool,
) error {

	logging.Info(
		ctx,
		"Createing tables for shards in %v",
		db.AbsolutePath(config.GetStorageFolder()),
	)

	err := db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"cluster_capacity",
		clusterCapacityStateTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}

	logging.Info(ctx, "Created cluster_capacity table")

	return nil
}

func DropYDBTables(
	ctx context.Context,
	config *shards_config.ShardsConfig,
	db *persistence.YDBClient,
) error {

	logging.Info(
		ctx,
		"Dropping tables for shards in %v",
		db.AbsolutePath(config.GetStorageFolder()),
	)

	err := db.DropTable(ctx, config.GetStorageFolder(), "cluster_capacity")
	if err != nil {
		return err
	}

	logging.Info(ctx, "Dropped cluster_capacity table")

	return nil
}
