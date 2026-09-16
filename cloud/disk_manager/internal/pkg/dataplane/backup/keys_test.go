package backup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestKeys(t *testing.T) {
	require.Equal(t, "chunks/task1.snap1.7", ChunkKey("", "task1.snap1.7"))
	require.Equal(t, "p/chunks/task1.snap1.7", ChunkKey("p", "task1.snap1.7"))
	require.Equal(t, "p/chunk_maps/snap1", ChunkMapKey("p", "snap1"))
	require.Equal(t, "p/snapshots/disk1/snap1/meta.json", SnapshotMetaKey("p", "disk1", "snap1"))
	require.Equal(t, "p/images/image1/meta.json", ImageMetaKey("p", "image1"))
}
