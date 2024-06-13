package tests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

////////////////////////////////////////////////////////////////////////////////

const (
	blockSize           = uint32(4096)
	blocksInChunk       = uint32(16)
	chunkSize           = blocksInChunk * blockSize
	chunkCount          = uint32(111)
	blockCount          = uint64(blocksInChunk * chunkCount)
	readerCount         = uint32(33)
	writerCount         = uint32(44)
	chunksInflightLimit = uint32(11) // should be much less than chunkCount

	shallowCopyWorkerCount   = uint32(11)
	shallowCopyInflightLimit = uint32(22) // should be much less than chunkCount
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

func newFactory(t *testing.T, ctx context.Context) nbs_client.Factory {
	rootCertsFile := os.Getenv("DISK_MANAGER_RECIPE_ROOT_CERTS_FILE")

	factory, err := nbs_client.NewFactory(
		ctx,
		&nbs_config.ClientConfig{
			Zones: map[string]*nbs_config.Zone{
				"zone": {
					Endpoints: []string{
						fmt.Sprintf(
							"localhost:%v",
							os.Getenv("DISK_MANAGER_RECIPE_NBS_PORT"),
						),
						fmt.Sprintf(
							"localhost:%v",
							os.Getenv("DISK_MANAGER_RECIPE_NBS_PORT"),
						),
					},
				},
			},
			RootCertsFile: &rootCertsFile,
		},
		metrics.NewEmptyRegistry(),
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	return factory
}

////////////////////////////////////////////////////////////////////////////////

func newDiskSourceFull(
	t *testing.T,
	ctx context.Context,
	factory nbs_client.Factory,
	disk *types.Disk,
	baseCheckpointID string,
	checkpointID string,
	duplicateChunkIndices bool,
	ignoreBaseDisk bool,
	dontReadFromCheckpoint bool,
) dataplane_common.Source {

	client, err := factory.GetClient(ctx, disk.ZoneId)
	require.NoError(t, err)

	err = client.CreateCheckpoint(
		ctx,
		nbs_client.CheckpointParams{
			DiskID:       disk.DiskId,
			CheckpointID: checkpointID,
		},
	)
	require.NoError(t, err)

	source, err := nbs.NewDiskSource(
		ctx,
		client,
		disk.DiskId,
		"",
		baseCheckpointID,
		checkpointID,
		nil, // encryption
		chunkSize,
		duplicateChunkIndices,
		ignoreBaseDisk,
		dontReadFromCheckpoint,
	)
	require.NoError(t, err)

	return source
}

func newDiskSource(
	t *testing.T,
	ctx context.Context,
	factory nbs_client.Factory,
	disk *types.Disk,
) dataplane_common.Source {

	return newDiskSourceFull(
		t,
		ctx,
		factory,
		disk,
		"",
		"checkpoint",
		false, // duplicateChunkIndices
		false, // ignoreBaseDisk
		false, // dontReadFromCheckpoint
	)
}

////////////////////////////////////////////////////////////////////////////////

func createDisk(
	t *testing.T,
	ctx context.Context,
	factory nbs_client.Factory,
	disk *types.Disk,
	diskBlockCount uint64,
) {

	client, err := factory.GetClient(ctx, disk.ZoneId)
	require.NoError(t, err)

	err = client.Create(ctx, nbs_client.CreateDiskParams{
		ID:          disk.DiskId,
		BlocksCount: diskBlockCount,
		BlockSize:   blockSize,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)
}

func fillDiskRange(
	t *testing.T,
	ctx context.Context,
	factory nbs_client.Factory,
	disk *types.Disk,
	checkpointID string,
	startIndex uint32,
	chunkCount uint32,
) (Resource, []dataplane_common.Chunk) {

	resource := Resource{
		newSource: func() dataplane_common.Source {
			return newDiskSourceFull(
				t,
				ctx,
				factory,
				disk,
				"",
				checkpointID,
				false, // duplicateChunkIndices
				false, // ignoreBaseDisk
				false, // dontReadFromCheckpoint
			)
		},
		newTarget: func() dataplane_common.Target {
			target, err := nbs.NewDiskTarget(
				ctx,
				factory,
				disk,
				nil,
				chunkSize,
				false, // ignoreZeroChunks
				0,     // fillGeneration
				0,     // fillSeqNumber
			)
			require.NoError(t, err)
			return target
		},
	}

	target := resource.newTarget()
	defer target.Close(ctx)

	chunks := test.FillTarget(t, ctx, target, chunkCount, chunkSize)

	return resource, chunks
}

func fillDisk(
	t *testing.T,
	ctx context.Context,
	factory nbs_client.Factory,
	disk *types.Disk,
	checkpointID string,
) (Resource, []dataplane_common.Chunk) {

	return fillDiskRange(t, ctx, factory, disk, checkpointID, 0, chunkCount)
}

////////////////////////////////////////////////////////////////////////////////

func newYDB(ctx context.Context) (*persistence.YDBClient, error) {
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
	)
	database := "/Root"
	connectionTimeout := "10s"

	return persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint:          &endpoint,
			Database:          &database,
			ConnectionTimeout: &connectionTimeout,
		},
		metrics.NewEmptyRegistry(),
	)
}

func newStorage(
	t *testing.T,
	ctx context.Context,
) (snapshot_storage.Storage, func()) {

	db, err := newYDB(ctx)
	require.NoError(t, err)

	closeFunc := func() {
		_ = db.Close(ctx)
	}

	storageFolder := fmt.Sprintf("transfer_integration_test/%v", t.Name())
	deleteWorkerCount := uint32(10)
	shallowCopyWorkerCount := shallowCopyWorkerCount
	shallowCopyInflightLimit := shallowCopyInflightLimit
	shardCount := uint64(2)
	s3Bucket := "test"
	chunkBlobsS3KeyPrefix := t.Name()

	config := &snapshot_config.SnapshotConfig{
		StorageFolder:             &storageFolder,
		DeleteWorkerCount:         &deleteWorkerCount,
		ShallowCopyWorkerCount:    &shallowCopyWorkerCount,
		ShallowCopyInflightLimit:  &shallowCopyInflightLimit,
		ChunkBlobsTableShardCount: &shardCount,
		ChunkMapTableShardCount:   &shardCount,
		S3Bucket:                  &s3Bucket,
		ChunkBlobsS3KeyPrefix:     &chunkBlobsS3KeyPrefix,
	}

	s3, err := test.NewS3Client()
	require.NoError(t, err)

	err = schema.Create(ctx, config, db, s3, false /* dropUnusedColumns */)
	require.NoError(t, err)

	storage, err := snapshot_storage.NewStorage(
		config,
		metrics.NewEmptyRegistry(),
		db,
		s3,
	)
	require.NoError(t, err)

	return storage, closeFunc
}

////////////////////////////////////////////////////////////////////////////////

type Resource struct {
	newSource        func() dataplane_common.Source
	newShallowSource func() dataplane_common.ShallowSource
	newTarget        func() dataplane_common.Target
}

func (r *Resource) fill(
	t *testing.T,
	ctx context.Context,
) []dataplane_common.Chunk {

	target := r.newTarget()
	defer target.Close(ctx)

	chunks := test.FillTarget(t, ctx, target, chunkCount, chunkSize)

	return chunks
}

func (r *Resource) checkChunks(
	t *testing.T,
	ctx context.Context,
	expectedChunks []dataplane_common.Chunk,
) {

	logging.Info(ctx, "Checking result...")

	source := r.newSource()
	defer source.Close(ctx)

	// TODO: hack that warms up source's caches.
	processedChunkIndices := make(chan uint32, 1)
	chunkIndices, _, _ := source.ChunkIndices(
		ctx,
		dataplane_common.Milestone{},
		processedChunkIndices,
		common.ChannelWithCancellation{}, // holeChunkIndices
	)
	for chunkIndex := range chunkIndices {
		processedChunkIndices <- chunkIndex
	}

	for _, expected := range expectedChunks {
		actual := dataplane_common.Chunk{
			Index: expected.Index,
			Data:  make([]byte, chunkSize),
		}
		err := source.Read(ctx, &actual)
		require.NoError(t, err)

		require.Equal(t, expected.Index, actual.Index)
		require.Equal(t, expected.Zero, actual.Zero)
		if !expected.Zero {
			require.Equal(t, expected.Data, actual.Data)
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

type transferTestCase struct {
	name               string
	useS3              bool
	withRandomFailures bool
}

func testCases() []transferTestCase {
	return []transferTestCase{
		{
			name:               "store chunks in ydb without random failures",
			useS3:              false,
			withRandomFailures: false,
		},
		{
			name:               "store chunks in s3 without random failures",
			useS3:              true,
			withRandomFailures: false,
		},
		{
			name:               "store chunks in ydb with random failures",
			useS3:              false,
			withRandomFailures: true,
		},
		{
			name:               "store chunks in s3 with random failures",
			useS3:              true,
			withRandomFailures: true,
		},
	}
}

////////////////////////////////////////////////////////////////////////////////

func transfer(
	t *testing.T,
	ctx context.Context,
	from Resource,
	to Resource,
	withRandomFailures bool,
) uint32 {

	logging.Info(
		ctx,
		"Starting transfer %v, withRandomFailures %v",
		t.Name(),
		withRandomFailures,
	)

	var milestoneChunkIndex uint32
	var transferredChunkCount uint32
	attemptIndex := 0

	attempt := func() error {
		source := from.newSource()
		defer source.Close(ctx)

		var shallowSource dataplane_common.ShallowSource
		if from.newShallowSource != nil {
			shallowSource = from.newShallowSource()
			defer shallowSource.Close(ctx)
		}

		target := to.newTarget()
		defer target.Close(ctx)

		logging.Info(
			ctx,
			"Attempt #%v, withRandomFailures %v",
			attemptIndex,
			withRandomFailures,
		)

		transferCtx, cancelTransferCtx := context.WithCancel(ctx)
		defer cancelTransferCtx()

		if withRandomFailures {
			go func() {
				for {
					select {
					case <-transferCtx.Done():
						return
					case <-time.After(2 * time.Second):
						if rand.Intn(2) == 0 {
							logging.Info(ctx, "Cancelling transfer context...")
							cancelTransferCtx()
						}
					}
				}
			}()
		}

		transferer := dataplane_common.Transferer{
			ReaderCount:         readerCount,
			WriterCount:         writerCount,
			ChunksInflightLimit: chunksInflightLimit,
			ChunkSize:           int(chunkSize),

			ShallowCopyWorkerCount:   shallowCopyWorkerCount,
			ShallowCopyInflightLimit: shallowCopyInflightLimit,
			ShallowSource:            shallowSource,
		}

		finalTransferredChunkCount, err := transferer.Transfer(
			transferCtx,
			source,
			target,
			dataplane_common.Milestone{
				ChunkIndex:            milestoneChunkIndex,
				TransferredChunkCount: transferredChunkCount,
			},
			func(ctx context.Context, milestone dataplane_common.Milestone) error {
				milestoneChunkIndex = milestone.ChunkIndex
				transferredChunkCount = milestone.TransferredChunkCount

				logging.Info(
					ctx,
					"Updating progress, milestoneChunkIndex %v, transferredChunkCount %v",
					milestoneChunkIndex,
					transferredChunkCount,
				)

				if withRandomFailures && rand.Intn(4) == 0 {
					return errors.NewRetriableErrorf(
						"emulated saveProgress error",
					)
				}

				return nil
			},
		)
		if err != nil {
			if transferCtx.Err() != nil {
				return errors.NewRetriableErrorf(
					"transfer context was cancelled",
				)
			}

			return err
		}

		transferredChunkCount = finalTransferredChunkCount
		return nil
	}

	for {
		err := attempt()
		if err == nil {
			return transferredChunkCount
		}

		if errors.CanRetry(err) {
			logging.Warn(ctx, "Attempt #%v failed: %v", attemptIndex, err)
			attemptIndex++
			continue
		}

		require.NoError(t, err)
	}
}

func fillAndTransfer(
	t *testing.T,
	ctx context.Context,
	from Resource,
	to Resource,
	withRandomFailures bool,
) {

	chunks := from.fill(t, ctx)

	transferredChunkCount := transfer(t, ctx, from, to, withRandomFailures)
	require.Equal(t, len(chunks), int(transferredChunkCount))
	to.checkChunks(t, ctx, chunks)
}

////////////////////////////////////////////////////////////////////////////////

func toDiskID(t *testing.T) string {
	return strings.ReplaceAll(t.Name(), "/", "_")
}

////////////////////////////////////////////////////////////////////////////////

func mergeChunks(
	baseChunks,
	chunks []dataplane_common.Chunk,
) []dataplane_common.Chunk {

	m := make(map[uint32]dataplane_common.Chunk)

	for _, chunk := range baseChunks {
		m[chunk.Index] = chunk
	}

	for _, chunk := range chunks {
		m[chunk.Index] = chunk
	}

	var res []dataplane_common.Chunk
	for _, chunk := range m {
		res = append(res, chunk)
	}

	return res
}

////////////////////////////////////////////////////////////////////////////////

func TestTransferFromDiskToDisk(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(newContext())
			defer cancel()

			factory := newFactory(t, ctx)

			diskID1 := fmt.Sprintf("%v_%v", toDiskID(t), 1)
			disk1 := &types.Disk{ZoneId: "zone", DiskId: diskID1}
			createDisk(t, ctx, factory, disk1, blockCount)

			diskID2 := fmt.Sprintf("%v_%v", toDiskID(t), 2)
			disk2 := &types.Disk{ZoneId: "zone", DiskId: diskID2}
			createDisk(t, ctx, factory, disk2, blockCount)

			from := Resource{
				newSource: func() dataplane_common.Source {
					return newDiskSource(t, ctx, factory, disk1)
				},
				newTarget: func() dataplane_common.Target {
					target, err := nbs.NewDiskTarget(
						ctx,
						factory,
						disk1,
						nil,
						chunkSize,
						false, // ignoreZeroChunks
						0,     // fillGeneration
						0,     // fillSeqNumber
					)
					require.NoError(t, err)
					return target
				},
			}

			to := Resource{
				newSource: func() dataplane_common.Source {
					return newDiskSource(t, ctx, factory, disk2)
				},
				newTarget: func() dataplane_common.Target {
					target, err := nbs.NewDiskTarget(
						ctx,
						factory,
						disk2,
						nil,
						chunkSize,
						false, // ignoreZeroChunks
						0,     // fillGeneration
						0,     // fillSeqNumber
					)
					require.NoError(t, err)
					return target
				},
			}

			fillAndTransfer(t, ctx, from, to, testCase.withRandomFailures)
		})
	}
}

func TestTransferFromDiskToSnapshot(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(newContext())
			defer cancel()

			storage, closeFunc := newStorage(t, ctx)
			defer closeFunc()

			factory := newFactory(t, ctx)

			diskID := toDiskID(t)
			disk := &types.Disk{ZoneId: "zone", DiskId: diskID}
			createDisk(t, ctx, factory, disk, blockCount)

			snapshotID := t.Name()

			from := Resource{
				newSource: func() dataplane_common.Source {
					return newDiskSource(t, ctx, factory, disk)
				},
				newTarget: func() dataplane_common.Target {
					target, err := nbs.NewDiskTarget(
						ctx,
						factory,
						disk,
						nil,
						chunkSize,
						false, // ignoreZeroChunks
						0,     // fillGeneration
						0,     // fillSeqNumber
					)
					require.NoError(t, err)
					return target
				},
			}

			to := Resource{
				newSource: func() dataplane_common.Source {
					return snapshot.NewSnapshotSource(snapshotID, storage)
				},
				newTarget: func() dataplane_common.Target {
					return snapshot.NewSnapshotTarget(
						"", // uniqueID
						snapshotID,
						storage,
						false, // ignoreZeroChunks
						testCase.useS3,
					)
				},
			}

			fillAndTransfer(t, ctx, from, to, testCase.withRandomFailures)
		})
	}
}

func TestTransferFromSnapshotToDisk(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(newContext())
			defer cancel()

			storage, closeFunc := newStorage(t, ctx)
			defer closeFunc()

			factory := newFactory(t, ctx)

			diskID := toDiskID(t)
			disk := &types.Disk{ZoneId: "zone", DiskId: diskID}
			createDisk(t, ctx, factory, disk, blockCount)

			snapshotID := t.Name()

			from := Resource{
				newSource: func() dataplane_common.Source {
					return snapshot.NewSnapshotSource(snapshotID, storage)
				},
				newTarget: func() dataplane_common.Target {
					return snapshot.NewSnapshotTarget(
						"", // uniqueID
						snapshotID,
						storage,
						false, // ignoreZeroChunks
						testCase.useS3,
					)
				},
			}

			to := Resource{
				newSource: func() dataplane_common.Source {
					return newDiskSource(t, ctx, factory, disk)
				},
				newTarget: func() dataplane_common.Target {
					target, err := nbs.NewDiskTarget(
						ctx,
						factory,
						disk,
						nil,
						chunkSize,
						false, // ignoreZeroChunks
						0,     // fillGeneration
						0,     // fillSeqNumber
					)
					require.NoError(t, err)
					return target
				},
			}

			fillAndTransfer(t, ctx, from, to, testCase.withRandomFailures)
		})
	}
}

func TestTransferFromSnapshotToSnapshot(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(newContext())
			defer cancel()

			storage, closeFunc := newStorage(t, ctx)
			defer closeFunc()

			snapshotID1 := fmt.Sprintf(t.Name(), 1)
			snapshotID2 := fmt.Sprintf(t.Name(), 2)

			from := Resource{
				newSource: func() dataplane_common.Source {
					return snapshot.NewSnapshotSource(snapshotID1, storage)
				},
				newTarget: func() dataplane_common.Target {
					return snapshot.NewSnapshotTarget(
						"", // uniqueID
						snapshotID1,
						storage,
						false, // ignoreZeroChunks
						testCase.useS3,
					)
				},
			}

			to := Resource{
				newSource: func() dataplane_common.Source {
					return snapshot.NewSnapshotSource(snapshotID2, storage)
				},
				newTarget: func() dataplane_common.Target {
					return snapshot.NewSnapshotTarget(
						"", // uniqueID
						snapshotID2,
						storage,
						false, // ignoreZeroChunks
						testCase.useS3,
					)
				},
			}

			fillAndTransfer(t, ctx, from, to, testCase.withRandomFailures)
		})
	}
}

func TestTransferFromDiskToIncrementalSnapshot(t *testing.T) {
	for _, testCase := range testCases() {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(newContext())
			defer cancel()

			storage, closeFunc := newStorage(t, ctx)
			defer closeFunc()

			factory := newFactory(t, ctx)

			diskID := toDiskID(t)
			disk := &types.Disk{ZoneId: "zone", DiskId: diskID}

			createDisk(t, ctx, factory, disk, blockCount)
			from, baseChunks := fillDisk(t, ctx, factory, disk, "base")

			baseSnapshotID := t.Name() + "_base"
			snapshotID := baseSnapshotID

			to := Resource{
				newSource: func() dataplane_common.Source {
					return snapshot.NewSnapshotSource(snapshotID, storage)
				},
				newTarget: func() dataplane_common.Target {
					return snapshot.NewSnapshotTarget(
						"", // uniqueID
						snapshotID,
						storage,
						false, // ignoreZeroChunks
						testCase.useS3,
					)
				},
			}

			_ = transfer(t, ctx, from, to, false)

			from.newSource = func() dataplane_common.Source {
				return newDiskSourceFull(
					t,
					ctx,
					factory,
					disk,
					"base",
					"incremental",
					true,  // duplicateChunkIndices
					false, // ignoreBaseDisk
					false, // dontReadFromCheckpoint
				)
			}

			snapshotID = t.Name() + "_incremental"

			from.newShallowSource = func() dataplane_common.ShallowSource {
				return snapshot.NewSnapshotShallowSource(
					baseSnapshotID,
					snapshotID,
					storage,
				)
			}

			chunks := from.fill(t, ctx)

			expectedChunks := mergeChunks(baseChunks, chunks)
			// TODO: check transferred chunk count.
			_ = transfer(t, ctx, from, to, testCase.withRandomFailures)
			to.checkChunks(t, ctx, expectedChunks)
		})
	}
}

// Test for NBS-4204.
func TestTransferFromDiskToIncrementalSnapshotWhenBaseSnapshotIsSmall(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	factory := newFactory(t, ctx)

	diskID := toDiskID(t)
	disk := &types.Disk{ZoneId: "zone", DiskId: diskID}

	createDisk(t, ctx, factory, disk, blockCount)
	from, baseChunks := fillDiskRange(
		t,
		ctx,
		factory,
		disk,
		"base",
		0,  // startIndex
		12, // chunkCount
	)

	baseSnapshotID := t.Name() + "_base"
	snapshotID := baseSnapshotID

	to := Resource{
		newSource: func() dataplane_common.Source {
			return snapshot.NewSnapshotSource(snapshotID, storage)
		},
		newTarget: func() dataplane_common.Target {
			return snapshot.NewSnapshotTarget(
				"", // uniqueID
				snapshotID,
				storage,
				false, // ignoreZeroChunks
				true,  // useS3
			)
		},
	}

	_ = transfer(t, ctx, from, to, false)

	from.newSource = func() dataplane_common.Source {
		return newDiskSourceFull(
			t,
			ctx,
			factory,
			disk,
			"base",
			"incremental",
			true,  // duplicateChunkIndices
			false, // ignoreBaseDisk
			false, // dontReadFromCheckpoint
		)
	}

	snapshotID = t.Name() + "_incremental"

	from.newShallowSource = func() dataplane_common.ShallowSource {
		return snapshot.NewSnapshotShallowSource(
			baseSnapshotID,
			snapshotID,
			storage,
		)
	}

	chunks := from.fill(t, ctx)

	expectedChunks := mergeChunks(baseChunks, chunks)
	// TODO: check transferred chunk count.
	_ = transfer(t, ctx, from, to, true)
	to.checkChunks(t, ctx, expectedChunks)
}

func TestTransferFromDiskToDiskWithFewChunksToTransfer(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	factory := newFactory(t, ctx)

	diskBlockCount := uint64(1 << 30)

	diskID1 := fmt.Sprintf("%v_%v", toDiskID(t), 1)
	disk1 := &types.Disk{ZoneId: "zone", DiskId: diskID1}
	createDisk(t, ctx, factory, disk1, diskBlockCount)

	diskID2 := fmt.Sprintf("%v_%v", toDiskID(t), 2)
	disk2 := &types.Disk{ZoneId: "zone", DiskId: diskID2}
	createDisk(t, ctx, factory, disk2, diskBlockCount)

	from := Resource{
		newSource: func() dataplane_common.Source {
			return newDiskSource(t, ctx, factory, disk1)
		},
		newTarget: func() dataplane_common.Target {
			target, err := nbs.NewDiskTarget(
				ctx,
				factory,
				disk1,
				nil,
				chunkSize,
				false, // ignoreZeroChunks
				0,     // fillGeneration
				0,     // fillSeqNumber
			)
			require.NoError(t, err)
			return target
		},
	}

	to := Resource{
		newSource: func() dataplane_common.Source {
			return newDiskSource(t, ctx, factory, disk2)
		},
		newTarget: func() dataplane_common.Target {
			target, err := nbs.NewDiskTarget(
				ctx,
				factory,
				disk2,
				nil,
				chunkSize,
				false, // ignoreZeroChunks
				0,     // fillGeneration
				0,     // fillSeqNumber
			)
			require.NoError(t, err)
			return target
		},
	}

	fillAndTransfer(t, ctx, from, to, true) // withRandomFailures
}
