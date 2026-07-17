package export

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
)

func TestPartitionChunkRange(t *testing.T) {
	testCases := []struct {
		name           string
		chunkCount     uint32
		partitionCount uint32
		expected       [][2]uint32
	}{
		{"single partition", 5, 1, [][2]uint32{{0, 5}}},
		{"even split", 6, 3, [][2]uint32{{0, 2}, {2, 4}, {4, 6}}},
		{"uneven split", 10, 3, [][2]uint32{{0, 4}, {4, 7}, {7, 10}}},
		{"more partitions than chunks", 3, 5, [][2]uint32{{0, 1}, {1, 2}, {2, 3}, {3, 3}, {3, 3}}},
		{"empty snapshot", 0, 3, [][2]uint32{{0, 0}, {0, 0}, {0, 0}}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			for i, expected := range testCase.expected {
				start, end, err := partitionChunkRange(
					testCase.chunkCount,
					uint32(i+1),
					testCase.partitionCount,
				)
				require.NoError(t, err)
				require.Equal(t, expected[0], start)
				require.Equal(t, expected[1], end)
			}
		})
	}
}

func TestExportPartitionRejectsInvalidArguments(t *testing.T) {
	testCases := []struct {
		name            string
		partition       uint32
		partitionCount  uint32
		readWorkerCount int
		errorSubstring  string
	}{
		{"zero partition count", 1, 0, testWorkerCount, "partitionCount must be positive"},
		{"zero partition", 0, 3, testWorkerCount, "partition must be in range"},
		{"partition greater than count", 4, 3, testWorkerCount, "partition must be in range"},
		{"zero read workers", 1, 1, 0, "readWorkerCount must be positive"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			snapshotStorage := mocks.NewStorageMock()
			var dst bytes.Buffer

			_, err := ExportPartitionToWriterWithReadWorkers(
				newContext(),
				snapshotStorage,
				"snapshot",
				&dst,
				testCase.partition,
				testCase.partitionCount,
				testCase.readWorkerCount,
			)
			require.Error(t, err)
			require.Contains(t, err.Error(), testCase.errorSubstring)
			snapshotStorage.AssertNumberOfCalls(t, "CheckSnapshotReady", 0)
		})
	}
}
