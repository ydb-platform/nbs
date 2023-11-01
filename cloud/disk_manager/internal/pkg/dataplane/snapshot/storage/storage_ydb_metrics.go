package storage

import (
	"context"
	"fmt"

	task_errors "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
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
		return 0, task_errors.NewNonRetriableErrorf(
			"GetDeletingSnapshotCount: failed to parse row: %w", err,
		)
	}
	return count, nil
}
