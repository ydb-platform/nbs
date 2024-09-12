package storage

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	"sync"
	"testing"
	"time"
)

func TestYdbHangingExternalBlobsAfterCance(t *testing.T) {
	ctx, cancel := context.WithCancel(test.NewContext())
	defer cancel()
	db, err := newYDB(ctx)
	require.NoError(t, err)

	folder := fmt.Sprintf("ydb_test/%v", t.Name())
	table := "table"

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		table,
		persistence.NewCreateTableDescription(
			persistence.WithColumn("shard_id", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("chunk_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("referer", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("data", persistence.Optional(persistence.TypeString)),
			persistence.WithColumn("refcnt", persistence.Optional(persistence.TypeUint32)),
			persistence.WithColumn("checksum", persistence.Optional(persistence.TypeUint32)),
			persistence.WithColumn("compression", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("shard_id", "chunk_id", "referer"),
			persistence.WithUniformPartitions(5),
			persistence.WithExternalBlobs("rotencrypted"),
		),
		false,
	)

	require.NoError(t, err)
	var wg sync.WaitGroup
	newCtx, newCancel := context.WithCancel(ctx)
	randomDataToWrite := make([][]byte, 0, 10)
	for i := 0; i < cap(randomDataToWrite); i++ {
		randomDataToWrite = append(
			randomDataToWrite,
			randomData(4096*1024, t),
		)
	}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(j int) {
			db.ExecuteRW(newCtx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $referer as Utf8;
		declare $chunk_id as Utf8;
		declare $data as String;
		declare $checksum as Uint32;
		declare $compression as Utf8;

		upsert into %v (shard_id, chunk_id, referer, data, refcnt, checksum, compression)
		values
			($shard_id, $chunk_id, "", $data, cast(1 as Uint32), $checksum, $compression),
			($shard_id, $chunk_id, $referer, null, null, null, null)
	`, db.AbsolutePath(folder), table),
				persistence.ValueParam("$shard_id", persistence.Uint64Value(uint64(j))),
				persistence.ValueParam("$chunk_id", persistence.UTF8Value(fmt.Sprintf("chunk_%d", j))),
				persistence.ValueParam("$referer", persistence.UTF8Value("sdsdsdsd")),
				persistence.ValueParam("$data", persistence.StringValue(randomDataToWrite[j%len(randomDataToWrite)])),
				persistence.ValueParam("$checksum", persistence.Uint32Value(uint32(1231231))),
				persistence.ValueParam("$compression", persistence.UTF8Value("lz4")),
			)
			wg.Done()
		}(i)
	}
	newCancel()
	wg.Wait()

	logging.Info(ctx, "After context was cancelled")
	secondContext, secondCancelFunc := context.WithTimeout(ctx, time.Minute*3)
	defer secondCancelFunc()
	for i := 100; i < 200; i++ {
		wg.Add(1)
		go func(j int) {
			now := time.Now()
			_, err := db.ExecuteRW(secondContext, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $referer as Utf8;
		declare $chunk_id as Utf8;
		declare $data as String;
		declare $checksum as Uint32;
		declare $compression as Utf8;

		upsert into %v (shard_id, chunk_id, referer, data, refcnt, checksum, compression)
		values
			($shard_id, $chunk_id, "", $data, cast(1 as Uint32), $checksum, $compression),
			($shard_id, $chunk_id, $referer, null, null, null, null)
	`, db.AbsolutePath(folder), table),
				persistence.ValueParam("$shard_id", persistence.Uint64Value(uint64(j))),
				persistence.ValueParam("$chunk_id", persistence.UTF8Value(fmt.Sprintf("chunk_%d", j))),
				persistence.ValueParam("$referer", persistence.UTF8Value("sdsdsdsd")),
				persistence.ValueParam("$data", persistence.StringValue(randomDataToWrite[j%len(randomDataToWrite)])),
				persistence.ValueParam("$checksum", persistence.Uint32Value(uint32(1231231))),
				persistence.ValueParam("$compression", persistence.UTF8Value("lz4")),
			)
			logging.Info(ctx, "Request for %d transaction been executed for %v", time.Now().Sub(now))
			require.NoError(t, err)
			wg.Done()
		}(i)
	}
	wg.Wait()
}
