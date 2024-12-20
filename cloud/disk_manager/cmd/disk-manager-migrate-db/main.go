package main

import (
	"context"
	"fmt"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/util"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
)

func splitArray[T any](arr []T, firstPartSize int) ([]T, []T) {
	var firstPart []T
	var remainingPart []T

	if len(arr) > firstPartSize {
		firstPart = arr[:firstPartSize]
		remainingPart = arr[firstPartSize:]
	} else {
		firstPart = arr
		remainingPart = []T{}
	}

	return firstPart, remainingPart
}

func getUserConfirmation(ctx context.Context, question string) bool {
	var response string
	for {
		fmt.Printf("%s (yes/no): ", question)
		if _, err := fmt.Scanln(&response); err != nil {
			continue
		}
		response = strings.ToLower(response)
		if response == "yes" || response == "no" || response == "y" ||
			response == "n" {
			break
		} else {
			fmt.Println("Please answer with yes or no")
		}
	}
	logging.Info(ctx, "Question: '%s', Response: '%s'", question, response)
	return response == "yes" || response == "y"
}

type chunkIndex struct {
	shardId uint64
	chunkId string
	referer string
	refcnt  uint32
}

func (c chunkIndex) String() string {
	return fmt.Sprintf(
		"<Chunk shard=%d, id=%s, referer=%s, refcnt=%d>",
		c.shardId,
		c.chunkId,
		c.referer,
		c.refcnt,
	)
}

func (c chunkIndex) GetChunkUniqueId() string {
	return strconv.FormatUint(c.shardId, 10) + c.chunkId + c.referer + strconv.FormatUint(uint64(c.refcnt), 10)
}

type Chunk struct {
	chunkIndex
	checksum    uint32
	data        []byte
	compression string
}

func scanChunkIndex(res persistence.Result) (chunk chunkIndex, err error) {
	err = res.ScanNamed(
		persistence.OptionalWithDefault("shard_id", &chunk.shardId),
		persistence.OptionalWithDefault("chunk_id", &chunk.chunkId),
		persistence.OptionalWithDefault("referer", &chunk.referer),
		persistence.OptionalWithDefault("refcnt", &chunk.refcnt),
	)
	return
}

func scanChunk(res persistence.Result) (chunk Chunk, err error) {
	err = res.ScanNamed(
		persistence.OptionalWithDefault("shard_id", &chunk.shardId),
		persistence.OptionalWithDefault("chunk_id", &chunk.chunkId),
		persistence.OptionalWithDefault("referer", &chunk.referer),
		persistence.OptionalWithDefault("refcnt", &chunk.refcnt),
		persistence.OptionalWithDefault("data", &chunk.data),
		persistence.OptionalWithDefault("checksum", &chunk.checksum),
		persistence.OptionalWithDefault("compression", &chunk.compression),
	)
	return
}

type chunkTransferer struct {
	sourceDatabase                            *persistence.YDBClient
	destinationDatabase                       *persistence.YDBClient
	sourceTablePrefix                         string
	destinationTablePrefix                    string
	destinationTableChunkBlobsTableShardCount uint64
	destinationTableExternalBlobsMediaKind    string
}

func newChunkTransferer(
	sourceDatabase *persistence.YDBClient,
	destinationDatabase *persistence.YDBClient,
) *chunkTransferer {

	tablePrefix := "snapshot"
	fromTablePrefix := sourceDatabase.AbsolutePath(tablePrefix)
	toTablePrefix := destinationDatabase.AbsolutePath(tablePrefix)
	return &chunkTransferer{
		sourceDatabase:                            sourceDatabase,
		destinationDatabase:                       destinationDatabase,
		sourceTablePrefix:                         fromTablePrefix,
		destinationTablePrefix:                    toTablePrefix,
		destinationTableChunkBlobsTableShardCount: 1000,
		destinationTableExternalBlobsMediaKind:    "rotencrypted",
	}
}

func (t *chunkTransferer) createOrAlterChunkBlobsTable(ctx context.Context) error {
	err := t.destinationDatabase.CreateOrAlterTable(
		ctx,
		"snapshot",
		"chunk_blobs",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("shard_id", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("chunk_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("referer", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("data", persistence.Optional(persistence.TypeString)),
			persistence.WithColumn("refcnt", persistence.Optional(persistence.TypeUint32)),
			persistence.WithColumn("checksum", persistence.Optional(persistence.TypeUint32)),
			persistence.WithColumn("compression", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("shard_id", "chunk_id", "referer"),
			persistence.WithUniformPartitions(t.destinationTableChunkBlobsTableShardCount),
			persistence.WithExternalBlobs(t.destinationTableExternalBlobsMediaKind),
		),
		false,
	)
	if err != nil {
		logging.Error(
			ctx,
			"Error while creating table in database %v",
			t.destinationDatabase,
		)
		return err
	}
	logging.Info(ctx, "Created chunk_blobs table")
	return nil
}

func listChunks(
	ctx context.Context,
	session *persistence.Session,
	tablePathPrefix string,
) (chunks []chunkIndex, err error) {

	res, err := session.StreamExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";

		select shard_id, chunk_id, referer, refcnt
		from chunk_blobs
	`, tablePathPrefix))
	if err != nil {
		return chunks, err
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			pool, err := scanChunkIndex(res)
			if err != nil {
				return chunks, err
			}

			chunks = append(chunks, pool)
		}
	}

	err = res.Err()
	if err != nil {
		return chunks, errors.NewRetriableError(err)
	}

	return chunks, nil
}

func ListChunks(
	ctx context.Context,
	db *persistence.YDBClient,
	tablePrefix string,
) ([]chunkIndex, error) {
	var result []chunkIndex
	var err error
	err = db.Execute(
		ctx, func(ctx context.Context, session *persistence.Session) error {
			result, err = listChunks(
				ctx,
				session,
				tablePrefix,
			)
			return err
		},
	)
	return result, err
}

func (t *chunkTransferer) loadChunk(ctx context.Context, c chunkIndex) (Chunk, error) {
	res, err := t.sourceDatabase.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $chunk_id as Utf8;

		select * from chunk_blobs
		where shard_id = $shard_id and
			chunk_id = $chunk_id and
			referer = $referer;
	`, t.sourceTablePrefix),
		persistence.ValueParam("$shard_id", persistence.Uint64Value(c.shardId)),
		persistence.ValueParam("$chunk_id", persistence.UTF8Value(c.chunkId)),
		persistence.ValueParam("$referer", persistence.UTF8Value(c.referer)),
	)
	if err != nil {
		return Chunk{}, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return Chunk{}, errors.NewNonRetriableErrorf("chunk not found")
	}

	return scanChunk(res)
}

func (t *chunkTransferer) saveChunk(ctx context.Context, chunk Chunk) error {
	_, err := t.destinationDatabase.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $shard_id as Uint64;
		declare $referer as Utf8;
		declare $chunk_id as Utf8;
		declare $data as String;
		declare $checksum as Uint32;
		declare $compression as Utf8;
		declare $refcnt as Uint32;

		upsert into chunk_blobs (shard_id, chunk_id, referer, data, refcnt, checksum, compression)
		values
			($shard_id, $chunk_id, $referer, $data, $refcnt, $checksum, $compression)`, t.destinationTablePrefix),
		persistence.ValueParam("$shard_id", persistence.Uint64Value(chunk.shardId)),
		persistence.ValueParam("$chunk_id", persistence.UTF8Value(chunk.chunkId)),
		persistence.ValueParam("$referer", persistence.UTF8Value(chunk.referer)),
		persistence.ValueParam("$data", persistence.StringValue(chunk.data)),
		persistence.ValueParam("$checksum", persistence.Uint32Value(chunk.checksum)),
		persistence.ValueParam("$refcnt", persistence.Uint32Value(chunk.refcnt)),
		persistence.ValueParam("$compression", persistence.UTF8Value(chunk.compression)),
	)
	return err
}

func (t *chunkTransferer) transferChunk(ctx context.Context, c chunkIndex) error {
	chunk, err := t.loadChunk(ctx, c)
	if err != nil {
		logging.Error(ctx, "Error while loading chunk %s: %v", chunk, err)
		return err
	}

	err = t.saveChunk(ctx, chunk)
	if err != nil {
		logging.Error(ctx, "Error while saving chunk %s: %v", chunk, err)
	}
	return err
}

func (t *chunkTransferer) deleteChunk(ctx context.Context, chunk chunkIndex) error {
	_, err := t.destinationDatabase.ExecuteRW(
		ctx,
		fmt.Sprintf(`--!syntax_v1
		pragma TablePathPrefix = "%v";
		pragma AnsiInForEmptyOrNullableItemsCollections;
		declare $shard_id as Uint64;
		declare $chunk_id as Utf8;
		declare $referer as Utf8;
		delete sourceDatabase chunk_map where shard_id = $shard_id and chunk_id = $chunk_id and $referer = referer
		`, t.sourceDatabase),
		persistence.ValueParam("$shard_id", persistence.Uint64Value(chunk.shardId)),
		persistence.ValueParam("$chunk_id", persistence.UTF8Value(chunk.chunkId)),
		persistence.ValueParam("$referer", persistence.UTF8Value(chunk.referer)),
	)
	if err != nil {
		logging.Error(ctx, "")
	}
	return err
}

func splitChunkIndices(chunks []chunkIndex) (big []chunkIndex, small []chunkIndex) {
	for _, c := range chunks {
		if c.referer != "" {
			small = append(small, c)
		} else {
			big = append(big, c)
		}

	}

	return
}

type parallelProcessor struct {
	wg, errWg     *sync.WaitGroup
	channel       chan chunkIndex
	errorsChannel chan error
	handler       func(ctx context.Context, index chunkIndex) error
	numWorkers    int
}

func newParallelProcessor(
	handler func(ctx context.Context, index chunkIndex) error,
	numWorkers int,
) parallelProcessor {

	var wg, errWg sync.WaitGroup
	return parallelProcessor{
		wg:            &wg,
		errWg:         &errWg,
		channel:       make(chan chunkIndex),
		errorsChannel: make(chan error),
		handler:       handler,
		numWorkers:    numWorkers,
	}

}

func (p *parallelProcessor) countErrors(ctx context.Context) {
	p.errWg.Add(1)
	defer p.errWg.Done()
	errorCounter := 0
	for {
		select {
		case _, ok := <-p.errorsChannel:
			if !ok {
				if errorCounter != 0 {
					logging.Error(ctx, "While processing data, %d errors occurred", errorCounter)
				}
				return
			}

			errorCounter++
		case <-ctx.Done():
			return
		}

	}

}

func (p *parallelProcessor) startWorkload(ctx context.Context) chan error {
	for i := 0; i < p.numWorkers; i++ {
		go func() {
			p.wg.Add(1)
			defer p.wg.Done()
			for {
				select {
				case chunkIndex, ok := <-p.channel:
					if !ok {
						return
					}

					err := p.handler(ctx, chunkIndex)
					if err != nil {
						p.errorsChannel <- err
					}

				case <-ctx.Done():
					return
				}

			}

		}()
	}

	return p.errorsChannel
}

func (p *parallelProcessor) enqueueChunks(
	ctx context.Context,
	chunkIndices []chunkIndex,
) {
	go func() {
		p.wg.Add(1)
		defer p.wg.Done()
		for _, c := range chunkIndices {
			select {
			case p.channel <- c:
				continue
			case <-ctx.Done():
				return
			}
		}
		close(p.channel)
	}()
}

func processChunkIndices(
	ctx context.Context,
	chunkIndices []chunkIndex,
	handler func(ctx context.Context, index chunkIndex) error,
	workersCount int,
) {
	processor := newParallelProcessor(handler, workersCount)
	go processor.enqueueChunks(ctx, chunkIndices)
	processor.startWorkload(ctx)
	go processor.countErrors(ctx)
	processor.Wait()
}

func (p *parallelProcessor) Wait() {
	p.wg.Wait()
	close(p.errorsChannel)
	p.errWg.Wait()
}

func logChunkIndices(
	ctx context.Context,
	prefix string,
	chunks []chunkIndex,
) {
	length := len(chunks)
	logging.Info(ctx, "%sCollected %d chunks", prefix, len(chunks))
	if length > 10 {
		length = 10
	}
	for i := 0; i < length; i++ {
		chunk := chunks[i]
		logging.Info(ctx, "Received %s", chunk)
	}
}

func (t *chunkTransferer) transferDb(ctx context.Context) error {
	existingChunksMap := make(map[string]chunkIndex)
	transferredChunksMap := make(map[string]chunkIndex)
	chunkIndicesInSource, err := ListChunks(ctx, t.sourceDatabase, t.sourceTablePrefix)
	if err != nil {
		return err
	}
	logChunkIndices(
		ctx,
		fmt.Sprintf(
			"from data source: %v, ",
			*t.sourceDatabase,
		),
		chunkIndicesInSource,
	)
	for _, c := range chunkIndicesInSource {
		existingChunksMap[c.GetChunkUniqueId()] = c
	}

	chunkIndicesInDestination, err := ListChunks(ctx, t.destinationDatabase, t.destinationTablePrefix)
	if err != nil {
		return err
	}
	logChunkIndices(
		ctx,
		fmt.Sprintf("from data source: %v, ", *t.destinationDatabase),
		chunkIndicesInDestination,
	)

	for _, c := range chunkIndicesInDestination {
		transferredChunksMap[c.GetChunkUniqueId()] = c
	}

	var chunkIndicesToCreate, toDelete []chunkIndex

	for _, c := range chunkIndicesInSource {
		if _, ok := transferredChunksMap[c.GetChunkUniqueId()]; !ok {
			chunkIndicesToCreate = append(chunkIndicesToCreate, c)
		}
	}

	logChunkIndices(ctx, "Chunks to delete", toDelete)
	for _, c := range chunkIndicesInDestination {
		if _, ok := existingChunksMap[c.GetChunkUniqueId()]; !ok {
			toDelete = append(toDelete, c)
		}
	}
	largeChunkIndicesToCreate, smallChunksToCreate := splitChunkIndices(chunkIndicesToCreate)
	logChunkIndices(ctx, "Large chunks to create", largeChunkIndicesToCreate)
	logChunkIndices(ctx, "Small chunks to create", smallChunksToCreate)
	if !getUserConfirmation(ctx, "Do you want to proceed with transfer?") {
		return nil
	}

	canaryToDelete, remainingToDelete := splitArray(toDelete, 1000)
	canarySmallChunksToCreate, remainingSmallChunksToCreate := splitArray(smallChunksToCreate, 1000)
	canaryLargeChunkIndicesToCreate, remainingLargeChunkIndicesToCreate := splitArray(largeChunkIndicesToCreate, 100)
	processChunkIndices(ctx, canaryToDelete, t.deleteChunk, 1000)
	processChunkIndices(ctx, canarySmallChunksToCreate, t.transferChunk, 1000)
	processChunkIndices(ctx, canaryLargeChunkIndicesToCreate, t.transferChunk, 100)
	if !getUserConfirmation(ctx, "Do you want to proceed with transfer?") {
		return nil
	}

	processChunkIndices(ctx, remainingToDelete, t.deleteChunk, 1000)
	processChunkIndices(ctx, remainingSmallChunksToCreate, t.transferChunk, 1000)
	processChunkIndices(ctx, remainingLargeChunkIndicesToCreate, t.transferChunk, 100)
	return nil
}

func main() {
	ctx := context.Background()
	ctx = logging.SetLogger(ctx, logging.NewStderrLogger(logging.DebugLevel))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		logging.Info(ctx, "Interrupt signal received, cancelling operations...")
		cancel()
	}()

	var sourceDbConfig, destinationDbConfig config.PersistenceConfig
	err := util.ParseProto("source.txt", &sourceDbConfig)
	if err != nil {
		logging.Fatal(ctx, "Error while parsing source config %v", err)
	}

	err = util.ParseProto("destination.txt", &destinationDbConfig)
	if err != nil {
		logging.Fatal(ctx, "Error while parsing destination config: %v", err)
	}

	sourceDatabase, err := persistence.NewYDBClient(
		ctx,
		&sourceDbConfig,
		metrics.NewEmptyRegistry(),
	)
	if err != nil {
		log.Fatal("Error:", err)
	}

	destinationDatabase, err := persistence.NewYDBClient(
		ctx,
		&destinationDbConfig,
		metrics.NewEmptyRegistry(),
		persistence.WithCredentials(credentials.NewAccessTokenCredentials(os.Getenv("IAM_TOKEN"))),
	)

	transferer := newChunkTransferer(sourceDatabase, destinationDatabase)
	transferer.createOrAlterChunkBlobsTable(ctx)
	transferer.transferDb(ctx)
}
