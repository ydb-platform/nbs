package tests

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

type writeRange struct {
	StartBlockIndex uint32
	BlockCount      uint32
}

type writeRequest struct {
	writeRange
	EndTimestamp uint32
}

// goWriteBlocksWithRequestLog issues write requests to the disk while building
// a request log for further validation.
//
// Each request is assigned a virtual start timestamp and end timestamp. The
// start timestamp is just the request's sequence number — it equals the
// request's index in the log.
//
// Writes are performed concurrently: one goroutine per range in `ranges`. Each
// request writes its startTimestamp into every block of the range, so that
// when the data is read, it is possible to determine exactly which request
// last wrote each block.
func goWriteBlocksWithRequestLog(
	ctx context.Context,
	client nbs.TestingClient,
	diskID string,
	ranges []writeRange,
) (func() ([]writeRequest, error), error) {

	session, err := client.MountRW(
		ctx,
		diskID,
		0,   // fillGeneration
		0,   // fillSeqNumber
		nil, // encryption
	)
	if err != nil {
		return nil, err
	}

	type requestWithStartTimestamp struct {
		writeRequest
		// Start timestamp of the request = the index of the request in the log.
		startTimestamp uint32
	}

	var currentTimestamp atomic.Uint32
	requests := make(chan requestWithStartTimestamp)

	cancelCtx, cancel := context.WithCancel(ctx)
	eg, egCtx := errgroup.WithContext(cancelCtx)

	var rangeIndex atomic.Uint32

	for i := 0; i < len(ranges); i++ {
		eg.Go(func() error {
			rng := ranges[rangeIndex.Add(1)-1]

			for {
				select {
				case <-egCtx.Done():
					return nil
				default:
				}

				startTimestamp := currentTimestamp.Add(1)
				blockSize := session.BlockSize()

				// Build data for the request: each block carries its own
				// request start timestamp encoded as a little-endian uint32.
				data := make([]byte, uint32(blockSize)*rng.BlockCount)
				for i := uint32(0); i < rng.BlockCount; i++ {
					blockData :=
						data[i*uint32(blockSize) : (i+1)*uint32(blockSize)]
					binary.LittleEndian.PutUint32(blockData, startTimestamp)
				}

				req := requestWithStartTimestamp{
					writeRequest: writeRequest{
						writeRange:   rng,
						EndTimestamp: math.MaxUint32,
					},
					startTimestamp: startTimestamp,
				}

				requests <- req

				err := session.Write(egCtx, uint64(rng.StartBlockIndex), data)
				if err != nil {
					return err
				}

				req.EndTimestamp = currentTimestamp.Load()
				requests <- req
			}
		})
	}

	go func() {
		_ = eg.Wait()
		session.Close(ctx)
		close(requests)
	}()

	requestLog := make([]writeRequest, 1, 10000)

	// Initially, all blocks are zero.
	requestLog[0] = writeRequest{
		writeRange: writeRange{
			StartBlockIndex: 0,
			BlockCount:      uint32(session.BlockCount()),
		},
		EndTimestamp: 0,
	}

	finished := make(chan struct{})

	go func() {
		for req := range requests {
			idx := int(req.startTimestamp)
			for len(requestLog) <= idx {
				requestLog = append(requestLog, writeRequest{})
			}
			requestLog[idx] = req.writeRequest
		}

		close(finished)
	}()

	stopWrites := func() ([]writeRequest, error) {
		cancel()
		<-finished
		return requestLog, nil
	}

	return stopWrites, nil
}

func requestToString(request writeRequest, startTimestamp uint32) string {
	return fmt.Sprintf(
		"StartBlockIndex=%v, BlockCount=%v, StartTimestamp=%v, EndTimestamp=%v",
		request.StartBlockIndex,
		request.BlockCount,
		startTimestamp,
		request.EndTimestamp,
	)
}

func ensureCheckpointValuesAreNotOutOfRange(
	ctx context.Context,
	log []writeRequest,
	data []uint32,
) bool {

	for blockIdx := uint32(0); blockIdx < uint32(len(data)); blockIdx++ {
		startTimestamp := data[blockIdx]
		request := log[startTimestamp]

		if !(blockIdx >= request.StartBlockIndex &&
			blockIdx < request.StartBlockIndex+request.BlockCount) {
			logging.Error(
				ctx,
				"The checkpoint data is inconsistent with the log: the value "+
					"%v was read from the %v-th block, but the request %v "+
					"does not touch this block.",
				startTimestamp,
				blockIdx,
				requestToString(request, startTimestamp),
			)
			return false
		}
	}

	return true
}

// validateCheckpointData checks for anomalies of the following kind:
//
// - Block 1 contains a value written by request A.
// - Request B also writes to block 1 and strictly follows request A.
// - Block 2 contains a value written by request C, and request C strictly
// follows request B.
//
// | Block | Value |                 Requests                    |
// |   1   |   A   |  <--req A-->  <--req B-->                   |
// |   2   |   C   |                              <--req C-->    |
//
// This is an anomaly: there is no point in time at which value A in block 1
// and value C in block 2 could have coexisted.
//
// Note: this function does NOT verify atomicity of multi-block writes.
//
// See TestCheckpointDataValidation for examples.
func validateCheckpointData(
	ctx context.Context,
	log []writeRequest,
	data []uint32,
) bool {

	logging.Info(ctx, "Log length is %v", len(log))
	for i := 0; i < len(log); i++ {
		logging.Debug(
			ctx,
			requestToString(log[i], uint32(i)),
		)
	}

	if !ensureCheckpointValuesAreNotOutOfRange(ctx, log, data) {
		return false
	}

	latestStartTimestampInCheckpointData := uint32(0)
	blockIdxWithLatestStartTimestamp := uint32(0)
	for blockIdx := uint32(0); blockIdx < uint32(len(data)); blockIdx++ {
		startTimestamp := data[blockIdx]

		logging.Debug(ctx, "Value at block %v is %v", blockIdx, startTimestamp)

		if startTimestamp > latestStartTimestampInCheckpointData {
			latestStartTimestampInCheckpointData = startTimestamp
			blockIdxWithLatestStartTimestamp = blockIdx
		}
	}

	for startTimestamp, req := range log {
		if req.EndTimestamp >= latestStartTimestampInCheckpointData {
			continue
		}

		rangeBegin := req.StartBlockIndex
		rangeEnd := req.StartBlockIndex + req.BlockCount

		for blockIdx := rangeBegin; blockIdx < rangeEnd; blockIdx++ {
			actualStartTimestamp := data[blockIdx]
			actualRequest := log[actualStartTimestamp]

			if uint32(startTimestamp) > actualRequest.EndTimestamp {
				logging.Error(
					ctx,
					"Anomaly detected: "+
						"value %v at block %v (written by request %v) and "+
						"value %v at block %v (written by request %v) "+
						"can not exist at the same time: "+
						"request %v must overwrite the value at block %v.",
					actualStartTimestamp,
					blockIdx,
					requestToString(actualRequest, actualStartTimestamp),
					latestStartTimestampInCheckpointData,
					blockIdxWithLatestStartTimestamp,
					requestToString(
						log[latestStartTimestampInCheckpointData],
						latestStartTimestampInCheckpointData),
					requestToString(req, uint32(startTimestamp)),
					blockIdx,
				)
				return false
			}
		}
	}

	return true
}

////////////////////////////////////////////////////////////////////////////////

func TestCheckpointDataValidation(t *testing.T) {
	ctx := newContext()

	log := []writeRequest{
		{writeRange{0, 2}, 0},
		{writeRange{0, 1}, 1},
		{writeRange{1, 1}, 2},
		{writeRange{0, 1}, 3},
		{writeRange{1, 1}, 4},
	}

	require.True(t, validateCheckpointData(ctx, log, []uint32{0, 0}))
	require.True(t, validateCheckpointData(ctx, log, []uint32{1, 0}))
	require.True(t, validateCheckpointData(ctx, log, []uint32{1, 2}))
	require.True(t, validateCheckpointData(ctx, log, []uint32{3, 2}))
	require.True(t, validateCheckpointData(ctx, log, []uint32{3, 4}))

	require.False(t, validateCheckpointData(ctx, log, []uint32{0, 2}))
	require.False(t, validateCheckpointData(ctx, log, []uint32{0, 4}))
	require.False(t, validateCheckpointData(ctx, log, []uint32{1, 4}))
	require.False(t, validateCheckpointData(ctx, log, []uint32{3, 0}))

	log = []writeRequest{
		{writeRange{0, 2}, 0},
		{writeRange{0, 1}, 1},
		{writeRange{1, 1}, 3},
		{writeRange{0, 1}, 3},
	}

	require.True(t, validateCheckpointData(ctx, log, []uint32{0, 0}))
	require.True(t, validateCheckpointData(ctx, log, []uint32{1, 0}))
	require.True(t, validateCheckpointData(ctx, log, []uint32{1, 2}))
	require.True(t, validateCheckpointData(ctx, log, []uint32{3, 0}))
	require.True(t, validateCheckpointData(ctx, log, []uint32{3, 2}))

	require.False(t, validateCheckpointData(ctx, log, []uint32{0, 2}))

	log = []writeRequest{
		{writeRange{0, 3}, 0},
		{writeRange{0, 2}, 1},
		{writeRange{1, 2}, 2},
	}

	require.True(t, validateCheckpointData(ctx, log, []uint32{0, 0, 0}))
	require.True(t, validateCheckpointData(ctx, log, []uint32{1, 1, 0}))
	require.True(t, validateCheckpointData(ctx, log, []uint32{1, 2, 2}))

	// Atomicity of multi-range writes is not checked.
	require.True(t, validateCheckpointData(ctx, log, []uint32{1, 0, 0}))
	require.True(t, validateCheckpointData(ctx, log, []uint32{0, 1, 0}))
	require.True(t, validateCheckpointData(ctx, log, []uint32{1, 2, 0}))
	require.True(t, validateCheckpointData(ctx, log, []uint32{1, 1, 2}))

	require.False(t, validateCheckpointData(ctx, log, []uint32{0, 2, 2}))
	require.False(t, validateCheckpointData(ctx, log, []uint32{0, 2, 0}))
	require.False(t, validateCheckpointData(ctx, log, []uint32{0, 0, 2}))
	require.False(t, validateCheckpointData(ctx, log, []uint32{1, 0, 2}))
	require.False(t, validateCheckpointData(ctx, log, []uint32{0, 1, 2}))
}

////////////////////////////////////////////////////////////////////////////////

func doTestCheckpointDataConsistency(
	t *testing.T,
	blocksCount uint32,
	ranges []writeRange,
) {

	ctx := newContext()

	client := newTestingClient(t, ctx)

	diskID := t.Name()
	blockSize := uint32(4096)

	err := client.Create(ctx, nbs.CreateDiskParams{
		ID:          diskID,
		BlocksCount: uint64(blocksCount),
		BlockSize:   blockSize,
		Kind:        types.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)

	stopWrites, err := goWriteBlocksWithRequestLog(
		ctx,
		client,
		diskID,
		ranges,
	)
	require.NoError(t, err)

	common.WaitForRandomDuration(200*time.Millisecond, 500*time.Millisecond)

	checkpointID := "checkpointID"
	err = client.CreateCheckpoint(ctx, nbs.CheckpointParams{
		DiskID:       diskID,
		CheckpointID: checkpointID,
	})
	require.NoError(t, err)

	log, err := stopWrites()
	require.NoError(t, err)

	session, err := client.MountRO(ctx, diskID, nil)
	require.NoError(t, err)
	defer session.Close(ctx)

	checkpointData := make([]uint32, blocksCount)
	for i := uint32(0); i < blocksCount; i++ {
		data := make([]byte, blockSize)
		var zero bool
		err = session.Read(ctx, uint64(i), 1, checkpointID, data, &zero)
		require.NoError(t, err)

		if !zero {
			checkpointData[i] = binary.LittleEndian.Uint32(data)
		}
	}

	require.True(t, validateCheckpointData(ctx, log, checkpointData))
}

func TestCheckpointDataConsistencySimple(t *testing.T) {
	doTestCheckpointDataConsistency(t, 2048, []writeRange{
		{StartBlockIndex: 0, BlockCount: 1},
		{StartBlockIndex: 1024, BlockCount: 1024},
	})
}

func TestCheckpointDataConsistencyComplicated(t *testing.T) {
	doTestCheckpointDataConsistency(t, 3072, []writeRange{
		{StartBlockIndex: 0, BlockCount: 1},
		{StartBlockIndex: 1, BlockCount: 1},
		{StartBlockIndex: 1024, BlockCount: 1},
		{StartBlockIndex: 0, BlockCount: 32},
		{StartBlockIndex: 1008, BlockCount: 32},
		{StartBlockIndex: 1024, BlockCount: 32},
		{StartBlockIndex: 0, BlockCount: 1024},
		{StartBlockIndex: 1024, BlockCount: 1024},
		{StartBlockIndex: 2048, BlockCount: 1024},
	})
}
