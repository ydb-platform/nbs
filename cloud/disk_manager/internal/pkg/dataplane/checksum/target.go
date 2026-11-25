package checksum

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"slices"
	"sort"
	"sync"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

// Checksum is CRC32 (IEEE) of the concatenation of 8-byte records
// (little-endian chunk index, little-endian chunk checksum) of all non-zero
// chunks with index less than NextChunkIndex, ordered by index.
// Zero chunks are not counted: the same contents might be stored with zero
// chunks, with data chunks full of zeroes or without these chunks at all
// (e.g. zero chunks are not copied when a snapshot is migrated), and the
// checksum should be the same in all these cases.
// Zero value is the state of the transfer that has not started yet.
// Checksums are stored persistently, so this format should not be changed.
type State struct {
	NextChunkIndex uint32
	Checksum       uint32
}

////////////////////////////////////////////////////////////////////////////////

type targetEntry struct {
	index    uint32
	checksum uint32
}

// Computes the checksum of the transferred chunks, does not store anything.
//
// Chunks are written in arbitrary order, so the written chunk is remembered
// (only its index and checksum) until the milestone of the transfer gets past
// it, then it is folded into the state, see GetState.
// The state has constant size and it is enough to continue after a restart:
// the target should be created from the saved state and the transfer should be
// resumed from its NextChunkIndex.
// Methods can be called concurrently.
type Target struct {
	// Chunks of the snapshot have indices less than this, other chunks are not
	// accepted.
	chunkCount uint32

	mutex sync.Mutex
	state State
	// Written non-zero chunks that are not folded into the state yet, ordered
	// by index. All indices are greater than or equal to state.NextChunkIndex.
	entries []targetEntry
}

var _ common.Target = (*Target)(nil)

func (t *Target) Write(ctx context.Context, chunk common.Chunk) error {
	// A snapshot has such a chunk only if its chunk map is damaged. Zero
	// chunks are checked as well.
	// The check also guarantees that NextChunkIndex is able to get past any
	// accepted chunk.
	if chunk.Index >= t.chunkCount {
		return errors.NewNonRetriableErrorf(
			"chunk index %v is out of range %v",
			chunk.Index,
			t.chunkCount,
		)
	}

	// Zero flag is checked first: data of zero chunk is garbage, source does
	// not fill it.
	if chunk.Zero || chunk.CheckDataIsAllZeroes() {
		// Zero chunks are not counted, so there is nothing to remember.
		return nil
	}

	entry := targetEntry{
		index: chunk.Index,
		// Data is reused after Write returns, so it should not be retained.
		checksum: chunk.Checksum(),
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	if entry.index < t.state.NextChunkIndex {
		// Already taken into account.
		return nil
	}

	i := sort.Search(len(t.entries), func(i int) bool {
		return t.entries[i].index >= entry.index
	})
	if i < len(t.entries) && t.entries[i].index == entry.index {
		if t.entries[i] != entry {
			return errors.NewNonRetriableErrorf(
				"chunk %v is written twice with different contents",
				entry.index,
			)
		}

		return nil
	}

	t.entries = slices.Insert(t.entries, i, entry)
	return nil
}

func (t *Target) Close(ctx context.Context) {
}

// Returns state to restore from.
//
// The checksum depends on the order of chunks, so a chunk can be folded into
// it only when nothing is going to be written before this chunk. That is what
// |milestone|, the milestone of the transfer, guarantees: all chunks that the
// source has before it are written already. So all written chunks that are
// before the milestone are folded in the order of their indices, and
// NextChunkIndex moves to the milestone: the indices that are skipped belong
// to the chunks that are zero or that the source does not have.
// The state never goes back: milestone that is behind NextChunkIndex changes
// nothing.
func (t *Target) GetState(milestone uint32) State {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.foldBefore(milestone)
	return t.state
}

// Should be called when the transfer is finished successfully: all chunks are
// written, so there is nothing to wait for and the rest of them is folded.
// The milestone is not enough for that, it might be behind the last chunks.
// NextChunkIndex gets past the last folded chunk.
func (t *Target) Finish() State {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if len(t.entries) != 0 {
		t.foldBefore(t.entries[len(t.entries)-1].index + 1)
	}

	return t.state
}

////////////////////////////////////////////////////////////////////////////////

// Folds written chunks with indices less than |chunkIndex| into the checksum
// in the order of their indices. NextChunkIndex becomes not less than
// |chunkIndex|.
func (t *Target) foldBefore(chunkIndex uint32) {
	foldedCount := 0
	for _, entry := range t.entries {
		if entry.index >= chunkIndex {
			break
		}

		var record [8]byte
		binary.LittleEndian.PutUint32(record[0:4], entry.index)
		binary.LittleEndian.PutUint32(record[4:8], entry.checksum)

		t.state.Checksum = crc32.Update(
			t.state.Checksum,
			crc32.IEEETable,
			record[:],
		)
		foldedCount++
	}

	// Folded entries are removed by moving the rest to the beginning, so the
	// memory of the slice is reused.
	t.entries = slices.Delete(t.entries, 0, foldedCount)
	t.state.NextChunkIndex = max(t.state.NextChunkIndex, chunkIndex)
}

////////////////////////////////////////////////////////////////////////////////

// |chunkCount| is the chunk count of the snapshot: writing a chunk with an
// index that is not less than it fails.
func NewTarget(state State, chunkCount uint32) *Target {
	return &Target{
		chunkCount: chunkCount,
		state:      state,
	}
}
