package storage

import (
	"context"
	"fmt"
	"time"

	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"

	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type storageYDB struct {
	db         *persistence.YDBClient
	tablesPath string
}

////////////////////////////////////////////////////////////////////////////////

func NewStorage(
	config *cells_config.CellsConfig,
	db *persistence.YDBClient,
) Storage {

	return &storageYDB{
		db:         db,
		tablesPath: db.AbsolutePath(config.GetStorageFolder()),
	}
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) AddClusterCapacities(
	ctx context.Context,
	capacities []ClusterCapacity,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.addClusterCapacities(ctx, session, capacities)
		},
	)
}

func (s *storageYDB) GetRecentClusterCapacities(
	ctx context.Context,
	zone_id string,
	kind types.DiskKind,
) ([]ClusterCapacity, error) {

	var capacities []ClusterCapacity
	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) (err error) {
			capacities, err = s.getRecentClusterCapacities(
				ctx,
				session,
				zone_id,
				kind,
			)
			return err
		},
	)
	return capacities, err
}

func (s *storageYDB) ClearOldClusterCapacities(
	ctx context.Context,
	createdBefore time.Time,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) (err error) {
			return s.clearOldClusterCapacities(
				ctx,
				session,
				createdBefore,
			)
		},
	)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) addClusterCapacities(ctx context.Context,
	session *persistence.Session,
	capacities []ClusterCapacity,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var values []persistence.Value
	addedAt := time.Now()

	for _, capacity := range capacities {
		capacityState := &clusterCapacityState{
			ZoneID:     capacity.ZoneID,
			CellID:     capacity.CellID,
			Kind:       common.DiskKindToString(capacity.Kind),
			TotalBytes: capacity.TotalBytes,
			FreeBytes:  capacity.FreeBytes,
			AddedAt:    addedAt,
		}

		values = append(values, capacityState.structValue())
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $capacities as List<%v>;

		upsert into cluster_capacity
		select *
		from AS_TABLE($capacities)
	`, s.tablesPath, clusterCapacityStateStructTypeString()),
		persistence.ValueParam("$capacities", persistence.ListValue(values...)),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) getRecentClusterCapacities(
	ctx context.Context,
	session *persistence.Session,
	zone_id string,
	kind types.DiskKind,
) ([]ClusterCapacity, error) {

	// Selecting only recent capacities from exact zone with exact kind.
	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $kind as Utf8;
		declare $zone_id as Utf8;

		$ordered = (
			SELECT
				t.*,
				ROW_NUMBER() OVER
				(PARTITION BY t.cell_id ORDER BY t.added_at DESC) AS row_number
			FROM cluster_capacity AS t
			WHERE t.zone_id = $zone_id and t.kind = $kind
		);
		SELECT *
		FROM $ordered
		WHERE row_number = 1
	`, s.tablesPath),
		persistence.ValueParam(
			"$kind",
			persistence.UTF8Value(common.DiskKindToString(kind)),
		),
		persistence.ValueParam("$zone_id", persistence.UTF8Value(zone_id)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	return scanClusterCapacities(ctx, res)
}

func (s *storageYDB) clearOldClusterCapacities(
	ctx context.Context,
	session *persistence.Session,
	createdBefore time.Time,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $created_before as Timestamp;

		delete from cluster_capacity
		where added_at < $created_before
	`, s.tablesPath),
		persistence.ValueParam("$created_before", persistence.TimestampValue(createdBefore)),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
