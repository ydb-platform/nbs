package storage

import (
	"context"
	"crypto/rand"
	"fmt"
	math_rand "math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type ydbTestFixture struct {
	t      *testing.T
	ctx    context.Context
	cancel context.CancelFunc
	db     *persistence.YDBClient
	table  string
	folder string
}

func newYdbTestFixture(t *testing.T) *ydbTestFixture {
	ctx, cancel := context.WithCancel(test.NewContext())
	db, err := newYDB(ctx)
	require.NoError(t, err)
	folder := fmt.Sprintf("ydb_test/%v", t.Name())
	table := "table"
	return &ydbTestFixture{
		t:      t,
		ctx:    ctx,
		cancel: cancel,
		db:     db,
		table:  table,
		folder: folder,
	}

}

func tableDescription() persistence.CreateTableDescription {
	optional := persistence.Optional
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("shard_id", optional(persistence.TypeUint64)),
		persistence.WithColumn("chunk_id", optional(persistence.TypeUTF8)),
		persistence.WithColumn("referer", optional(persistence.TypeUTF8)),
		persistence.WithColumn("data", optional(persistence.TypeString)),
		persistence.WithColumn("refcnt", optional(persistence.TypeUint32)),
		persistence.WithColumn("checksum", optional(persistence.TypeUint32)),
		persistence.WithColumn("compression", optional(persistence.TypeUTF8)),
		persistence.WithPrimaryKeyColumn("shard_id", "chunk_id", "referer"),
		persistence.WithUniformPartitions(5),
		persistence.WithExternalBlobs("rotencrypted"),
	)
}

func (f *ydbTestFixture) initSchema() {
	err := f.db.CreateOrAlterTable(
		f.ctx,
		f.folder,
		f.table,
		tableDescription(),
		false,
	)
	require.NoError(f.t, err)
}

func (f *ydbTestFixture) close() {
	require.NoError(f.t, f.db.Close(f.ctx))
	f.cancel()
}

func (f *ydbTestFixture) writeChunkData(
	ctx context.Context,
	chunkIndex int,
	dataToWrite []byte,
) {

	f.db.ExecuteRW(ctx, fmt.Sprintf(`
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
	`, f.db.AbsolutePath(f.folder), f.table),
		persistence.ValueParam(
			"$shard_id", persistence.Uint64Value(uint64(chunkIndex))),
		persistence.ValueParam(
			"$chunk_id",
			persistence.UTF8Value(fmt.Sprintf("chunk_%d", chunkIndex)),
		),
		persistence.ValueParam("$referer", persistence.UTF8Value("sdsdsdsd")),
		persistence.ValueParam(
			"$data",
			persistence.StringValue(dataToWrite),
		),
		persistence.ValueParam(
			"$checksum",
			persistence.Uint32Value(uint32(1231231)),
		),
		persistence.ValueParam("$compression", persistence.UTF8Value("lz4")),
	)
}

////////////////////////////////////////////////////////////////////////////////

func randomData(size int, t *testing.T) []byte {
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	return data
}

func sleepRandomDuration() {
	durations := []time.Duration{
		10 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		500 * time.Millisecond,
	}

	time.Sleep(durations[math_rand.Intn(len(durations))])
}
func TestYdbHangingRequest(t *testing.T) {
	randomDataToWrite := make([][]byte, 0, 10)
	for i := 0; i < cap(randomDataToWrite); i++ {
		randomDataToWrite = append(
			randomDataToWrite,
			randomData(4096*1024, t),
		)
	}

	for i := 0; i < 50; i++ {
		func() {
			f := newYdbTestFixture(t)
			defer f.cancel()
			f.initSchema()
			launchAndCancelParallelTransactions(f, randomDataToWrite)
			waitForTransactionsHanging(f, randomDataToWrite)
		}()
	}

}

func waitForTransactionsHanging(f *ydbTestFixture, randomDataToWrite [][]byte) {
	var wg sync.WaitGroup
	logging.Info(f.ctx, "Next transactions can be hanging")
	transactionDuration := time.Minute * 3
	secondContext, secondCancelFunc := context.WithTimeout(
		f.ctx,
		transactionDuration,
	)
	defer secondCancelFunc()

	for chunkInd := 100; chunkInd < 200; chunkInd++ {
		wg.Add(1)
		go func(chunkIndex int) {
			now := time.Now()
			dataToWrite := randomDataToWrite[chunkIndex%len(randomDataToWrite)]
			f.writeChunkData(secondContext, chunkIndex, dataToWrite)
			duration := time.Now().Sub(now)
			logging.Info(
				f.ctx,
				"Request for %d transaction been executed for %v",
				chunkIndex,
				duration,
			)
			if duration > transactionDuration {
				panic("Hanging request to YDB")
			}

			wg.Done()
		}(chunkInd)
	}
	wg.Wait()
}

func launchAndCancelParallelTransactions(
	f *ydbTestFixture,
	randomDataToWrite [][]byte,
) {

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(f.ctx)
	for chunkInd := 0; chunkInd < 100; chunkInd++ {
		wg.Add(1)
		go func(chunkIndex int) {
			dataToWrite := randomDataToWrite[chunkIndex%len(randomDataToWrite)]
			f.writeChunkData(ctx, chunkIndex, dataToWrite)
			wg.Done()
		}(chunkInd)
	}

	sleepRandomDuration()
	cancel()
	wg.Wait()
}
