package storage

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) GetDeletingSnapshotCount(ctx context.Context) (count uint64, err error) {
	defer s.metrics.StatOperation("GetDeletingSnapshotCount")(&err)

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";

		select count(*)
		from deleting
	`, s.tablesPath,
	))
	if err != nil {
		return 0, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return 0, nil
	}

	err = res.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (s *storageYDB) GetSnapshotCount(ctx context.Context) (count uint64, err error) {
	defer s.metrics.StatOperation("GetSnapshotCount")(&err)

	res, err := s.db.ExecuteRO(
		ctx,
		fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $status as Int64;

		select count(*)
		from snapshots
		where status = $status;
	`, s.tablesPath),
		persistence.ValueParam(
			"$status",
			persistence.Int64Value(int64(snapshotStatusReady)),
		),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return 0, nil
	}

	err = res.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (s *storageYDB) GetTotalSnapshotSize(ctx context.Context) (size uint64, err error) {
	defer s.metrics.StatOperation("GetTotalSnapshotSize")(&err)

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $status as Int64;

		select sum(size)
		from snapshots
		where status = $status;
	`, s.tablesPath),
		persistence.ValueParam(
			"$status",
			persistence.Int64Value(int64(snapshotStatusReady)),
		),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return 0, nil
	}

	err = res.ScanWithDefaults(&size)
	if err != nil {
		return 0, err
	}

	return size, nil
}

func (s *storageYDB) GetTotalSnapshotStorageSize(ctx context.Context) (storageSize uint64, err error) {
	defer s.metrics.StatOperation("GetTotalSnapshotStorageSize")(&err)

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $status as Int64;

		select sum(storage_size)
		from snapshots
		where status = $status;
	`, s.tablesPath),
		persistence.ValueParam(
			"$status",
			persistence.Int64Value(int64(snapshotStatusReady)),
		),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return 0, nil
	}

	err = res.ScanWithDefaults(&storageSize)
	if err != nil {
		return 0, err
	}

	return storageSize, nil
}
