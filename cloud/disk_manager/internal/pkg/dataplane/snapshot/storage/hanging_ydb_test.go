package storage

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
	math_rand "math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func randomData(size int, t *testing.T) []byte {
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	return data
}

func TestYdbHangingRequest(t *testing.T) {
	for i := 0; i < 50; i++ {
		oneTestIteration(t)
	}
}

func oneTestIteration(t *testing.T) {
	ctx, cancel := context.WithCancel(test.NewContext())
	defer cancel()
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
	)
	database := "/Root"
	rootPath := "disk_manager"
	connectionTimeout := "10s"
	db, err := persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint:          &endpoint,
			Database:          &database,
			RootPath:          &rootPath,
			ConnectionTimeout: &connectionTimeout,
		},
		metrics.NewEmptyRegistry(),
	)
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
	durations := []time.Duration{
		10 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		500 * time.Millisecond,
	}
	time.Sleep(durations[math_rand.Intn(len(durations))])
	newCancel()
	wg.Wait()

	logging.Info(ctx, "After context was cancelled")
	transactionDuration := time.Minute * 3
	secondContext, secondCancelFunc := context.WithTimeout(ctx, transactionDuration)
	defer secondCancelFunc()
	for i := 100; i < 200; i++ {
		wg.Add(1)
		go func(j int) {
			now := time.Now()
			db.ExecuteRW(secondContext, fmt.Sprintf(`
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
			duration := time.Now().Sub(now)
			logging.Info(ctx, "Request for %d transaction been executed for %v", duration)
			require.Less(t, duration, transactionDuration)
			wg.Done()
		}(i)
	}
	wg.Wait()
}
