package storage

import (
	"context"
	"time"

	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type ClusterCapacity struct {
	ZoneID     string
	CellID     string
	Kind       types.DiskKind
	TotalBytes uint64
	FreeBytes  uint64
}

////////////////////////////////////////////////////////////////////////////////

type clusterCapacityState struct {
	ZoneID     string
	CellID     string
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
		ZoneID:     c.ZoneID,
		CellID:     c.CellID,
		Kind:       kind,
		TotalBytes: c.TotalBytes,
		FreeBytes:  c.FreeBytes,
	}, nil
}

// Returns ydb entity of the node object.
func (c *clusterCapacityState) structValue() persistence.Value {
	return persistence.StructValue(
		persistence.StructFieldValue("zone_id", persistence.UTF8Value(c.ZoneID)),
		persistence.StructFieldValue("cell_id", persistence.UTF8Value(c.CellID)),
		persistence.StructFieldValue("kind", persistence.UTF8Value(c.Kind)),
		persistence.StructFieldValue("total_bytes", persistence.Uint64Value(c.TotalBytes)),
		persistence.StructFieldValue("free_bytes", persistence.Uint64Value(c.FreeBytes)),
		persistence.StructFieldValue("created_at", persistence.TimestampValue(c.CreatedAt)),
	)
}

////////////////////////////////////////////////////////////////////////////////

func clusterCapacityStateStructTypeString() string {
	return `Struct<
		zone_id: Utf8,
		cell_id: Utf8,
		kind: Utf8,
		total_bytes: Uint64,
		free_bytes: Uint64,
		created_at: Timestamp>`
}

func clusterCapacityStateTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("cell_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("kind", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("total_bytes", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("free_bytes", persistence.Optional(persistence.TypeUint64)),

		persistence.WithColumn("created_at", persistence.Optional(persistence.TypeTimestamp)),

		persistence.WithPrimaryKeyColumn("kind", "cell_id", "created_at"),
	)
}

func scanClusterCapacity(
	res persistence.Result,
) (capacity clusterCapacityState, err error) {

	err = res.ScanNamed(
		persistence.OptionalWithDefault("zone_id", &capacity.ZoneID),
		persistence.OptionalWithDefault("cell_id", &capacity.CellID),
		persistence.OptionalWithDefault("kind", &capacity.Kind),
		persistence.OptionalWithDefault("total_bytes", &capacity.TotalBytes),
		persistence.OptionalWithDefault("free_bytes", &capacity.FreeBytes),
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
	UpdateClusterCapacities(
		ctx context.Context,
		capacities []ClusterCapacity,
		deleteBefore time.Time,
	) error

	GetRecentClusterCapacities(
		ctx context.Context,
		zone_id string,
		kind types.DiskKind,
	) ([]ClusterCapacity, error)
}

////////////////////////////////////////////////////////////////////////////////

func CreateYDBTables(
	ctx context.Context,
	config *cells_config.CellsConfig,
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
	config *cells_config.CellsConfig,
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
