package checksum

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

const testChunkSize = 16

func newDataChunk(chunkIndex uint32, value byte) dataplane_common.Chunk {
	return dataplane_common.Chunk{
		Index: chunkIndex,
		Data:  bytes.Repeat([]byte{value}, testChunkSize),
	}
}

// Data of zero chunk is garbage: source does not fill it.
func newZeroChunk(chunkIndex uint32) dataplane_common.Chunk {
	chunk := newDataChunk(chunkIndex, 0xff)
	chunk.Zero = true
	return chunk
}

////////////////////////////////////////////////////////////////////////////////

// Zero chunk is either marked as zero or has data of zeroes.
func isZeroChunk(chunk dataplane_common.Chunk) bool {
	return chunk.Zero || bytes.Count(chunk.Data, []byte{0}) == len(chunk.Data)
}

func appendExpectedRecord(
	records []byte,
	chunk dataplane_common.Chunk,
) []byte {

	if isZeroChunk(chunk) {
		// Zero chunk is the same as the absent one.
		return records
	}

	records = binary.LittleEndian.AppendUint32(records, chunk.Index)
	return binary.LittleEndian.AppendUint32(
		records,
		crc32.ChecksumIEEE(chunk.Data),
	)
}

// Does not share the code with the target: the checksum is computed in one go
// from all the records.
// Chunks should be ordered by index.
func expectedChecksum(chunks ...dataplane_common.Chunk) uint32 {
	var records []byte
	for _, chunk := range chunks {
		records = appendExpectedRecord(records, chunk)
	}

	return crc32.ChecksumIEEE(records)
}

////////////////////////////////////////////////////////////////////////////////

// Operation on the target together with the check of its result.
type testOp func(t *testing.T, target *Target, description string)

func TestChecksumTarget(t *testing.T) {
	ctx := context.Background()

	write := func(chunkIndex uint32, value byte) testOp {
		return func(t *testing.T, target *Target, description string) {
			err := target.Write(ctx, newDataChunk(chunkIndex, value))
			require.NoError(t, err, description)
		}
	}

	writeZero := func(chunkIndex uint32) testOp {
		return func(t *testing.T, target *Target, description string) {
			err := target.Write(ctx, newZeroChunk(chunkIndex))
			require.NoError(t, err, description)
		}
	}

	failToWrite := func(chunk dataplane_common.Chunk, message string) testOp {
		return func(t *testing.T, target *Target, description string) {
			err := target.Write(ctx, chunk)
			require.Error(t, err, description)
			require.True(
				t,
				errors.Is(err, errors.NewEmptyNonRetriableError()),
				description,
			)
			require.ErrorContains(t, err, message, description)
		}
	}

	// Takes the state with |milestone| and checks that it has the given
	// NextChunkIndex and Checksum. Also checks the count of written chunks
	// that are not folded yet.
	getState := func(
		milestone uint32,
		nextChunkIndex uint32,
		checksum uint32,
		pendingCount int,
	) testOp {

		return func(t *testing.T, target *Target, description string) {
			require.Equal(
				t,
				State{NextChunkIndex: nextChunkIndex, Checksum: checksum},
				target.GetState(milestone),
				description,
			)
			require.Len(t, target.entries, pendingCount, description)
		}
	}

	finish := func(nextChunkIndex uint32, checksum uint32) testOp {
		return func(t *testing.T, target *Target, description string) {
			require.Equal(
				t,
				State{NextChunkIndex: nextChunkIndex, Checksum: checksum},
				target.Finish(),
				description,
			)
			require.Empty(t, target.entries, description)
		}
	}

	closeTarget := func() testOp {
		return func(t *testing.T, target *Target, description string) {
			target.Close(ctx)
		}
	}

	a, b, c := byte(1), byte(2), byte(3)

	s0 := expectedChecksum(newDataChunk(0, a))
	s01 := expectedChecksum(newDataChunk(0, a), newDataChunk(1, b))
	s012 := expectedChecksum(
		newDataChunk(0, a),
		newDataChunk(1, b),
		newDataChunk(2, c),
	)
	s015 := expectedChecksum(
		newDataChunk(0, a),
		newDataChunk(1, b),
		newDataChunk(5, c),
	)
	s02 := expectedChecksum(newDataChunk(0, a), newDataChunk(2, b))
	s05 := expectedChecksum(newDataChunk(0, a), newDataChunk(5, c))
	s3 := expectedChecksum(newDataChunk(3, a))
	sLast := expectedChecksum(newDataChunk(math.MaxUint32-1, a))

	// The same data with another index gives another checksum.
	require.NotEqual(t, s0, s3)

	testCases := []struct {
		name string
		// State to create the target from.
		state State
		ops   []testOp
	}{
		{
			name: "empty",
			ops:  []testOp{getState(0, 0, 0, 0), finish(0, 0)},
		},
		{
			name: "chunks in order",
			ops:  []testOp{write(0, a), write(1, b), getState(2, 2, s01, 0)},
		},
		{
			name: "chunks out of order",
			ops: []testOp{
				write(2, c), write(0, a), write(1, b), getState(3, 3, s012, 0),
			},
		},
		{
			name: "chunk is folded when the milestone gets past it",
			ops: []testOp{
				write(0, a), write(1, b), write(5, c),
				getState(0, 0, 0, 3),
				getState(1, 1, s0, 2),
				// Chunk 5 is not before the milestone.
				getState(5, 5, s01, 1),
				getState(6, 6, s015, 0),
			},
		},
		{
			name: "milestone behind the state changes nothing",
			ops: []testOp{
				write(0, a), write(1, b), getState(2, 2, s01, 0),
				write(5, c), getState(1, 2, s01, 1), getState(0, 2, s01, 1),
				getState(6, 6, s015, 0), getState(2, 6, s015, 0),
			},
		},
		{
			name: "milestone without chunks",
			ops:  []testOp{getState(10, 10, 0, 0), finish(10, 0)},
		},
		// Zero chunk, absent chunk (hole) and data chunk of zeroes are the same
		// thing: the next three cases end up with the same state.
		{
			name: "zero chunk",
			ops: []testOp{
				write(0, a), writeZero(1), write(2, b), getState(3, 3, s02, 0),
			},
		},
		{
			name: "hole instead of the zero chunk",
			ops:  []testOp{write(0, a), write(2, b), getState(3, 3, s02, 0)},
		},
		{
			name: "data chunk of zeroes instead of the zero chunk",
			ops: []testOp{
				write(0, a), write(1, 0), write(2, b), getState(3, 3, s02, 0),
			},
		},
		{
			// The same as the empty target.
			name: "zero chunks are not remembered",
			ops: []testOp{
				writeZero(0), write(1, 0), writeZero(7),
				getState(0, 0, 0, 0), finish(0, 0),
			},
		},
		{
			name: "hole at the beginning",
			ops: []testOp{
				write(3, a), getState(3, 3, 0, 1), getState(4, 4, s3, 0),
			},
		},
		{
			name: "holes at the end",
			ops: []testOp{
				write(0, a), getState(100, 100, s0, 0), finish(100, s0),
			},
		},
		{
			name:  "restored state",
			state: State{NextChunkIndex: 2, Checksum: s01},
			ops: []testOp{
				getState(0, 2, s01, 0), write(5, c), getState(6, 6, s015, 0),
			},
		},
		{
			name:  "restored state ignores chunks before it",
			state: State{NextChunkIndex: 2, Checksum: s01},
			ops: []testOp{
				write(0, c), write(1, c), getState(2, 2, s01, 0),
				write(2, c), getState(3, 3, s012, 0),
			},
		},
		{
			name: "finish folds the rest",
			ops: []testOp{
				write(0, a), write(1, b), write(5, c), getState(1, 1, s0, 2),
				finish(6, s015), finish(6, s015), getState(6, 6, s015, 0),
			},
		},
		{
			name: "finish without the milestone",
			ops:  []testOp{write(5, c), write(0, a), finish(6, s05)},
		},
		{
			name: "finish after the milestone that is past the last chunk",
			ops:  []testOp{write(0, a), getState(9, 9, s0, 0), finish(9, s0)},
		},
		{
			// Nothing but the milestone gets NextChunkIndex past zero chunks,
			// and the last milestone of the transfer might be behind its last
			// chunks.
			name: "finish does not get past zero chunks at the end",
			ops: []testOp{
				write(0, a), write(1, b), writeZero(2), write(3, 0),
				getState(1, 1, s0, 1), finish(2, s01), getState(2, 2, s01, 0),
			},
		},
		{
			name:  "finish of a restored state",
			state: State{NextChunkIndex: 7, Checksum: 12345},
			ops:   []testOp{finish(7, 12345), getState(7, 7, 12345, 0)},
		},
		{
			name: "the same chunk twice",
			ops: []testOp{
				write(1, b), write(1, b), write(0, a),
				getState(0, 0, 0, 2), getState(2, 2, s01, 0),
			},
		},
		{
			name: "different chunks with one index",
			ops: []testOp{
				write(1, b),
				failToWrite(newDataChunk(1, c), "written twice"),
				// The first one stays.
				write(0, a), getState(2, 2, s01, 0),
			},
		},
		{
			// Zero chunks are not remembered, so there is nothing to compare
			// them with.
			name: "zero chunk and data chunk with one index",
			ops: []testOp{
				writeZero(1), write(1, b), writeZero(1), write(0, a),
				getState(2, 2, s01, 0),
			},
		},
		{
			name: "chunk is written again after it was folded",
			ops: []testOp{
				write(0, a), getState(1, 1, s0, 0),
				write(0, a), write(0, c), writeZero(0), getState(1, 1, s0, 0),
				finish(1, s0),
			},
		},
		{
			name: "close does not fold anything",
			ops: []testOp{
				write(0, a), write(5, c), closeTarget(),
				getState(1, 1, s0, 1), finish(6, s05),
			},
		},
		{
			name: "chunk index is too big",
			ops: []testOp{
				failToWrite(newDataChunk(math.MaxUint32, a), "too big"),
				failToWrite(newZeroChunk(math.MaxUint32), "too big"),
				getState(0, 0, 0, 0),
				write(math.MaxUint32-1, a), finish(math.MaxUint32, sLast),
			},
		},
		{
			name: "milestone after the biggest chunk index",
			ops: []testOp{
				write(math.MaxUint32-1, a),
				getState(math.MaxUint32-1, math.MaxUint32-1, 0, 1),
				getState(math.MaxUint32, math.MaxUint32, sLast, 0),
				finish(math.MaxUint32, sLast),
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			target := NewTarget(testCase.state)
			defer target.Close(ctx)

			for i, op := range testCase.ops {
				op(t, target, fmt.Sprintf("op #%v", i))
			}
		})
	}
}

// Checksums are stored persistently, so the values that are hardcoded here
// should never change.
func TestChecksumTargetChecksumFormat(t *testing.T) {
	ctx := context.Background()

	newChunk := func(chunkIndex uint32, value byte) dataplane_common.Chunk {
		return dataplane_common.Chunk{
			Index: chunkIndex,
			Data:  bytes.Repeat([]byte{value}, 16),
		}
	}

	// Each non-zero chunk gives the record: little-endian index and
	// little-endian CRC32 (IEEE) of the data. The checksum is CRC32 (IEEE) of
	// all the records.
	records := []byte{
		// Chunk 0, CRC32 of its data (16 bytes of 0x01) is 0x52a028b7.
		0x00, 0x00, 0x00, 0x00, 0xb7, 0x28, 0xa0, 0x52,
		// Chunk 1, CRC32 of its data (16 bytes of 0x02) is 0x4bfc8ad0.
		0x01, 0x00, 0x00, 0x00, 0xd0, 0x8a, 0xfc, 0x4b,
		// Chunk 5, CRC32 of its data (16 bytes of 0x03) is 0xf5e7e932.
		0x05, 0x00, 0x00, 0x00, 0x32, 0xe9, 0xe7, 0xf5,
	}

	require.Equal(t, uint32(0xf8bb3223), crc32.ChecksumIEEE(records[:8]))
	require.Equal(t, uint32(0x7a35e775), crc32.ChecksumIEEE(records[:16]))
	require.Equal(t, uint32(0xc85900ad), crc32.ChecksumIEEE(records))

	// Expectations of the other tests are computed in the same way.
	require.Equal(
		t,
		uint32(0xc85900ad),
		expectedChecksum(
			newChunk(0, 0x01),
			newChunk(1, 0x02),
			newZeroChunk(2),
			newChunk(4, 0x00),
			newChunk(5, 0x03),
		),
	)

	target := NewTarget(State{})
	require.Equal(t, State{}, target.GetState(0))

	err := target.Write(ctx, newChunk(0, 0x01))
	require.NoError(t, err)
	require.Equal(
		t,
		State{NextChunkIndex: 1, Checksum: 0xf8bb3223},
		target.GetState(1),
	)

	err = target.Write(ctx, newChunk(1, 0x02))
	require.NoError(t, err)
	require.Equal(
		t,
		State{NextChunkIndex: 2, Checksum: 0x7a35e775},
		target.GetState(2),
	)

	// Neither zero chunk nor absent chunks (3 and 4) have records.
	err = target.Write(ctx, newZeroChunk(2))
	require.NoError(t, err)
	require.Equal(
		t,
		State{NextChunkIndex: 3, Checksum: 0x7a35e775},
		target.GetState(3),
	)

	err = target.Write(ctx, newChunk(5, 0x03))
	require.NoError(t, err)
	require.Equal(
		t,
		State{NextChunkIndex: 6, Checksum: 0xc85900ad},
		target.Finish(),
	)

	// The state that was saved by the previous version should be continued
	// in the same way.
	target = NewTarget(State{NextChunkIndex: 2, Checksum: 0x7a35e775})
	err = target.Write(ctx, newChunk(5, 0x03))
	require.NoError(t, err)
	require.Equal(
		t,
		State{NextChunkIndex: 6, Checksum: 0xc85900ad},
		target.Finish(),
	)

	// All bytes of the index are different here.
	record := []byte{0x04, 0x03, 0x02, 0x01, 0xb7, 0x28, 0xa0, 0x52}
	require.Equal(t, uint32(0x3db1aeff), crc32.ChecksumIEEE(record))

	target = NewTarget(State{})
	err = target.Write(ctx, newChunk(0x01020304, 0x01))
	require.NoError(t, err)
	require.Equal(
		t,
		State{NextChunkIndex: 0x01020305, Checksum: 0x3db1aeff},
		target.Finish(),
	)
}

// Any change of the snapshot contents should change the checksum.
func TestChecksumTargetChecksumDependsOnContents(t *testing.T) {
	ctx := context.Background()

	computeChecksum := func(
		t *testing.T,
		chunks ...dataplane_common.Chunk,
	) uint32 {

		target := NewTarget(State{})
		defer target.Close(ctx)

		for _, chunk := range chunks {
			err := target.Write(ctx, chunk)
			require.NoError(t, err)
		}

		state := target.Finish()
		require.Equal(t, expectedChecksum(chunks...), state.Checksum)
		return state.Checksum
	}

	type chunks = []dataplane_common.Chunk
	data := newDataChunk
	zero := newZeroChunk

	checksum := computeChecksum(t, data(0, 1), data(1, 2), zero(2), data(4, 3))

	testCases := []struct {
		name   string
		chunks chunks
	}{
		{
			name:   "chunk is changed",
			chunks: chunks{data(0, 1), data(1, 4), zero(2), data(4, 3)},
		},
		{
			name:   "chunk is replaced with zero chunk",
			chunks: chunks{data(0, 1), zero(1), zero(2), data(4, 3)},
		},
		{
			name:   "chunk is lost",
			chunks: chunks{data(0, 1), zero(2), data(4, 3)},
		},
		{
			name:   "last chunk is lost",
			chunks: chunks{data(0, 1), data(1, 2), zero(2)},
		},
		{
			name:   "all chunks are lost",
			chunks: chunks{},
		},
		{
			name:   "chunk is moved",
			chunks: chunks{data(0, 1), data(1, 2), zero(2), data(3, 3)},
		},
		{
			name:   "chunks are swapped",
			chunks: chunks{data(0, 2), data(1, 1), zero(2), data(4, 3)},
		},
		{
			name:   "zero chunk is replaced with data",
			chunks: chunks{data(0, 1), data(1, 2), data(2, 3), data(4, 3)},
		},
		{
			name: "hole is replaced with data",
			chunks: chunks{
				data(0, 1), data(1, 2), zero(2), data(3, 3), data(4, 3),
			},
		},
		{
			name: "chunk is added to the end",
			chunks: chunks{
				data(0, 1), data(1, 2), zero(2), data(4, 3), data(5, 3),
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.NotEqual(
				t,
				checksum,
				computeChecksum(t, testCase.chunks...),
			)
		})
	}

	t.Run("bit of data is flipped", func(t *testing.T) {
		for i := 0; i < testChunkSize*8; i++ {
			chunk := data(1, 2)
			chunk.Data[i/8] ^= 1 << (i % 8)

			require.NotEqual(
				t,
				checksum,
				computeChecksum(t, data(0, 1), chunk, zero(2), data(4, 3)),
				"bit %v",
				i,
			)
		}
	})

	t.Run("byte of data of zeroes is changed", func(t *testing.T) {
		// Chunk has the real size: all its data should be examined.
		chunkSize := 4 * 1024 * 1024
		chunk := dataplane_common.Chunk{Data: make([]byte, chunkSize)}
		require.Zero(t, computeChecksum(t, chunk))

		for _, i := range []int{0, chunkSize / 2, chunkSize - 1} {
			chunk.Data[i] = 1
			require.NotZero(t, computeChecksum(t, chunk), "byte %v", i)
			chunk.Data[i] = 0
		}
	})
}

////////////////////////////////////////////////////////////////////////////////

// Chunks of the snapshot ordered by index together with the expected results
// of their transfer. Snapshot does not have chunks for some indices.
type testSnapshot struct {
	chunks []dataplane_common.Chunk
	// checksums[i] is the checksum of chunks[:i].
	checksums []uint32
}

func newTestSnapshot(chunks ...dataplane_common.Chunk) *testSnapshot {
	snapshot := &testSnapshot{
		chunks:    chunks,
		checksums: []uint32{expectedChecksum()},
	}

	var records []byte
	for _, chunk := range chunks {
		records = appendExpectedRecord(records, chunk)
		snapshot.checksums = append(
			snapshot.checksums,
			crc32.ChecksumIEEE(records),
		)
	}

	return snapshot
}

func newRandomTestSnapshot(random *rand.Rand, chunkCount int) *testSnapshot {
	var chunks []dataplane_common.Chunk
	chunkIndex := uint32(0)

	for len(chunks) < chunkCount {
		switch random.Intn(8) {
		case 0:
			// Several absent chunks.
			chunkIndex += uint32(random.Intn(10))
		case 1:
			chunks = append(chunks, newZeroChunk(chunkIndex))
		case 2:
			chunks = append(chunks, newDataChunk(chunkIndex, 0))
		default:
			value := byte(1 + random.Intn(255))
			chunks = append(chunks, newDataChunk(chunkIndex, value))
		}

		chunkIndex++
	}

	return newTestSnapshot(chunks...)
}

// Returns chunks that are read by the transfer started from |chunkIndex|.
func (s *testSnapshot) chunksFrom(
	chunkIndex uint32,
) []dataplane_common.Chunk {

	i := sort.Search(len(s.chunks), func(i int) bool {
		return s.chunks[i].Index >= chunkIndex
	})
	return s.chunks[i:]
}

// Returns the state that takes into account all the chunks with index less
// than |nextChunkIndex| and nothing else.
func (s *testSnapshot) expectedState(nextChunkIndex uint32) State {
	chunkCount := len(s.chunks) - len(s.chunksFrom(nextChunkIndex))
	return State{
		NextChunkIndex: nextChunkIndex,
		Checksum:       s.checksums[chunkCount],
	}
}

// Returns the checksum of the whole snapshot.
func (s *testSnapshot) expectedChecksum() uint32 {
	return s.checksums[len(s.chunks)]
}

// Returns milestone of the transfer started from |startChunkIndex|: index of
// the first chunk that is not written yet.
func (s *testSnapshot) milestone(
	startChunkIndex uint32,
	written map[uint32]bool,
) uint32 {

	milestone := startChunkIndex
	for _, chunk := range s.chunksFrom(startChunkIndex) {
		if !written[chunk.Index] {
			return chunk.Index
		}

		milestone = chunk.Index + 1
	}

	return milestone
}

// Does what the transfer resumed from |state| does: creates the target from
// |state| and writes the chunks with index not less than its NextChunkIndex,
// keeping the order of |chunks|.
// Takes the state after each write and after the finish and checks it: the
// expected state is computed here independently of the target.
// Returns new states in the order they were taken.
func (s *testSnapshot) transfer(
	t *testing.T,
	state State,
	chunks []dataplane_common.Chunk,
	outdatedMilestone bool,
) []State {

	ctx := context.Background()
	target := NewTarget(state)
	defer target.Close(ctx)

	written := make(map[uint32]bool)
	milestone := state.NextChunkIndex
	// Milestone that the target gets.
	targetMilestone := state.NextChunkIndex

	var writtenChunkIndices []uint32
	var description []interface{}
	lastState := state
	var newStates []State

	for _, chunk := range chunks {
		if chunk.Index < state.NextChunkIndex {
			continue
		}

		writtenChunkIndices = append(writtenChunkIndices, chunk.Index)
		description = []interface{}{
			"state %+v, outdated milestone %v, written chunks %v",
			state,
			outdatedMilestone,
			writtenChunkIndices,
		}

		err := target.Write(ctx, chunk)
		require.NoError(t, err, description...)
		written[chunk.Index] = true

		// Milestone of the real transfer is updated asynchronously, so the
		// target might get the one that does not know about the last write.
		targetMilestone = milestone
		milestone = s.milestone(state.NextChunkIndex, written)
		if !outdatedMilestone {
			targetMilestone = milestone
		}

		// What is expected: exactly the chunks before the milestone are
		// folded, the rest of the written non-zero chunks wait for it.
		pendingCount := 0
		for _, pendingChunk := range s.chunksFrom(targetMilestone) {
			if written[pendingChunk.Index] && !isZeroChunk(pendingChunk) {
				pendingCount++
			}
		}

		newState := target.GetState(targetMilestone)
		require.Equal(
			t,
			s.expectedState(targetMilestone),
			newState,
			description...,
		)
		require.Len(t, target.entries, pendingCount, description...)

		if newState != lastState {
			newStates = append(newStates, newState)
			lastState = newState
		}
	}

	// All chunks are written, so nothing is left behind wherever the milestone
	// is. NextChunkIndex gets past the last chunk that has waited for it.
	nextChunkIndex := targetMilestone
	for _, pendingChunk := range s.chunksFrom(targetMilestone) {
		if !isZeroChunk(pendingChunk) {
			nextChunkIndex = pendingChunk.Index + 1
		}
	}

	require.Equal(
		t,
		State{NextChunkIndex: nextChunkIndex, Checksum: s.expectedChecksum()},
		target.Finish(),
		description...,
	)
	require.Empty(t, target.entries, description...)

	return newStates
}

// Calls |visit| for each permutation of |chunks|. The slice is reused, so it
// should not be retained.
func forEachPermutation(
	chunks []dataplane_common.Chunk,
	visit func(chunks []dataplane_common.Chunk),
) {

	permutation := append([]dataplane_common.Chunk(nil), chunks...)

	var permute func(count int)
	permute = func(count int) {
		if count <= 1 {
			visit(permutation)
			return
		}

		for i := 0; i < count; i++ {
			permutation[i], permutation[count-1] =
				permutation[count-1], permutation[i]
			permute(count - 1)
			permutation[i], permutation[count-1] =
				permutation[count-1], permutation[i]
		}
	}

	permute(len(permutation))
}

// Chunks of a small snapshot are written in each possible order. Whatever the
// order is, the state should cover exactly the chunks before the milestone,
// and the target that is created from any saved state should end up with the
// same checksum as the one that was not restarted.
func TestChecksumTargetRestoreFromEachState(t *testing.T) {
	snapshot := newTestSnapshot(
		// Chunk 0 is a hole.
		newDataChunk(1, 1),
		newZeroChunk(2),
		// Chunk 3 is a hole.
		newDataChunk(4, 2),
		newDataChunk(5, 0),
		newDataChunk(6, 3),
		newZeroChunk(8),
	)

	forEachPermutation(
		snapshot.chunks,
		func(chunks []dataplane_common.Chunk) {
			for _, outdatedMilestone := range []bool{false, true} {
				states := snapshot.transfer(
					t,
					State{},
					chunks,
					outdatedMilestone,
				)

				for _, state := range states {
					// Written chunks that were not folded are lost, so the
					// transfer reads them again.
					snapshot.transfer(t, state, chunks, outdatedMilestone)
				}
			}
		},
	)
}

////////////////////////////////////////////////////////////////////////////////

// Does what the transfer does: chunk indices are generated in order and go
// through the inflight queue that computes the milestone, chunks are written
// by several goroutines that reuse their buffers and the state is taken
// concurrently with the writes.
// Returns the state of the finished transfer.
func (s *testSnapshot) transferConcurrently(t *testing.T) State {
	const writerCount = 8
	const inflightLimit = 32

	ctx := context.Background()
	target := NewTarget(State{})
	defer target.Close(ctx)

	chunksToWrite := make(chan dataplane_common.Chunk, inflightLimit)
	writtenChunkIndices := make(chan uint32, inflightLimit)
	queue := common.NewInflightQueue(
		common.Milestone{},
		writtenChunkIndices,
		common.ChannelWithCancellation{}, // holeValues
		inflightLimit,
	)

	var writers sync.WaitGroup
	for i := 0; i < writerCount; i++ {
		writers.Add(1)

		go func() {
			defer writers.Done()

			garbage := bytes.Repeat([]byte{0xff}, testChunkSize)
			data := make([]byte, testChunkSize)

			for chunk := range chunksToWrite {
				if chunk.Index%3 == 0 {
					// Let the next chunks be written before this one.
					runtime.Gosched()
				}

				// Data is valid only during the write and zero chunk has
				// garbage instead of data.
				copy(data, garbage)
				if !chunk.Zero {
					copy(data, chunk.Data)
				}

				err := target.Write(ctx, dataplane_common.Chunk{
					Index: chunk.Index,
					Data:  data,
					Zero:  chunk.Zero,
				})
				assert.NoError(t, err)
				copy(data, garbage)

				// The milestone does not get past the chunk until this moment.
				writtenChunkIndices <- chunk.Index
			}
		}()
	}

	stopSaver := make(chan struct{})
	var saver sync.WaitGroup
	saver.Add(1)

	go func() {
		defer saver.Done()

		previousState := State{}
		for !t.Failed() {
			select {
			case <-stopSaver:
				return
			default:
			}

			// All chunks before the milestone are written.
			milestone := queue.Milestone().Value
			state := target.GetState(milestone)

			assert.Equal(t, s.expectedState(state.NextChunkIndex), state)
			// Otherwise the transfer that is restarted from the saved state
			// reads the chunks before the milestone once again.
			assert.GreaterOrEqual(t, state.NextChunkIndex, milestone)
			assert.GreaterOrEqual(
				t,
				state.NextChunkIndex,
				previousState.NextChunkIndex,
			)

			previousState = state
			runtime.Gosched()
		}
	}()

	for _, chunk := range s.chunks {
		ok, err := queue.Add(ctx, chunk.Index)
		require.NoError(t, err)
		require.True(t, ok)

		chunksToWrite <- chunk
	}

	close(chunksToWrite)
	writers.Wait()
	close(writtenChunkIndices)

	// The state is still being taken at this moment.
	finalState := target.Finish()
	close(stopSaver)
	saver.Wait()

	return finalState
}

func TestChecksumTargetConcurrentWrites(t *testing.T) {
	random := rand.New(rand.NewSource(1))

	for iteration := 0; iteration < 20; iteration++ {
		snapshot := newRandomTestSnapshot(random, 2000 /* chunkCount */)

		state := snapshot.transferConcurrently(t)
		require.Equal(t, snapshot.expectedState(state.NextChunkIndex), state)
		require.Equal(t, snapshot.expectedChecksum(), state.Checksum)
	}
}
