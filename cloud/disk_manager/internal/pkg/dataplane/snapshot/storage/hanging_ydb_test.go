package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	mathrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

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
		persistence.WithColumn("data", optional(persistence.TypeString)),
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
) error {

	_, err := f.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $chunk_id as Utf8;
		declare $data as String;

		upsert into %v (shard_id, chunk_id, data)
		values ($shard_id, $chunk_id, $data)
	`, f.db.AbsolutePath(f.folder), f.table),
		persistence.ValueParam(
			"$shard_id", persistence.Uint64Value(uint64(chunkIndex))),
		persistence.ValueParam(
			"$chunk_id",
			persistence.UTF8Value(fmt.Sprintf("chunk_%d", chunkIndex)),
		),
		persistence.ValueParam(
			"$data",
			persistence.StringValue(dataToWrite),
		),
	)

	if errors.Is(err, context.Canceled) {
		return nil
	}

	return err
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

	time.Sleep(durations[mathrand.Intn(len(durations))])
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
	logging.Info(f.ctx, "Next transactions can be hanging")
	transactionDuration := time.Minute * 3
	secondContext, secondCancelFunc := context.WithTimeout(
		f.ctx,
		transactionDuration,
	)
	defer secondCancelFunc()
	var errGrp errgroup.Group

	for chunkInd := 100; chunkInd < 200; chunkInd++ {
		chunkIndex := chunkInd
		errGrp.Go(
			func() error {
				now := time.Now()
				dataToWrite := randomDataToWrite[chunkIndex%len(randomDataToWrite)]
				err := f.writeChunkData(secondContext, chunkIndex, dataToWrite)
				if err != nil {
					return err
				}

				duration := time.Now().Sub(now)
				logging.Info(
					f.ctx,
					"Request for %d transaction been executed for %v",
					chunkIndex,
					duration,
				)
				if duration > transactionDuration {
					return fmt.Errorf("hanging request to YDB")
				}

				return nil
			},
		)
	}
	require.NoError(f.t, errGrp.Wait())
}

func launchAndCancelParallelTransactions(
	f *ydbTestFixture,
	randomDataToWrite [][]byte,
) {

	var errGrp errgroup.Group
	ctx, cancel := context.WithCancel(f.ctx)
	for chunkInd := 0; chunkInd < 100; chunkInd++ {
		chunkIndex := chunkInd
		errGrp.Go(
			func() error {
				dataToWrite := randomDataToWrite[chunkIndex%len(randomDataToWrite)]
				return f.writeChunkData(ctx, chunkIndex, dataToWrite)
			},
		)
	}

	sleepRandomDuration()
	cancel()
	require.NoError(f.t, errGrp.Wait())
}
