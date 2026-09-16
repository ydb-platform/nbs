package backup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestKeys(t *testing.T) {
	require.Equal(t, "chunks/task1.snap1.7", ChunkKey("", "task1.snap1.7"))
	require.Equal(t, "p/chunks/task1.snap1.7", ChunkKey("p", "task1.snap1.7"))
	require.Equal(t, "p/snapshots/disk1/snap1/meta.json", MetaKey("p", "disk1", "snap1"))
	require.Equal(t, "p/snapshots/disk1/snap1/map.bin", ChunkMapKey("p", "disk1", "snap1"))
	require.Equal(t, "snapshots/-/snap1/meta.json", MetaKey("", "", "snap1"))
}
