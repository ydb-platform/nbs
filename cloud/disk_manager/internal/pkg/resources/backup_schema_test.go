package resources

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

func TestBackupResourceSchemaDropRemovesQueues(t *testing.T) {
	ctx := newContext()
	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)
	for _, c := range []struct {
		name, column string
		create       func(string) error
		drop         func(string) error
	}{
		{"images", "image_id", func(p string) error { return createImagesYDBTables(ctx, p, db, false) }, func(p string) error { return dropImagesYDBTables(ctx, p, db) }},
		{"snapshots", "snapshot_id", func(p string) error { return createSnapshotsYDBTables(ctx, p, db, false) }, func(p string) error { return dropSnapshotsYDBTables(ctx, p, db) }},
	} {
		folder := "backup_resource_schema/" + t.Name() + "/" + c.name
		require.NoError(t, c.create(folder))
		query := fmt.Sprintf("UPSERT INTO `%s/backup_queue` (%s) VALUES ($id);", db.AbsolutePath(folder), c.column)
		result, err := db.ExecuteRW(ctx, "DECLARE $id AS Utf8; "+query, persistence.ValueParam("$id", persistence.UTF8Value("resource")))
		require.NoError(t, err)
		result.Close()
		require.NoError(t, c.drop(folder))
		require.NoError(t, c.drop(folder))
		result, err = db.ExecuteRO(ctx, fmt.Sprintf("SELECT * FROM `%s/backup_queue`;", db.AbsolutePath(folder)))
		if err == nil {
			result.Close()
		}
		require.Error(t, err)
		require.NoError(t, c.create(folder))
	}
}
