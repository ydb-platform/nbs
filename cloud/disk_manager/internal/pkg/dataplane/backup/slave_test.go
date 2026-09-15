package backup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestSlaveObjects(t *testing.T) {
	require.Equal(t, "chunks/task1.snap1.7", chunkObject("task1.snap1.7"))
	require.Equal(t, "snapshots/disk1/snap1/meta.json", metaObject("disk1", "snap1"))
	require.Equal(t, "snapshots/disk1/snap1/map.bin", mapObject("disk1", "snap1"))
	require.Equal(t, "snapshots/-/snap1/meta.json", metaObject("", "snap1"))
}

func TestSlaveKey(t *testing.T) {
	require.Equal(t, "chunks/c1", newSlave(nil, "b", "").key("chunks/c1"))
	require.Equal(t, "p/chunks/c1", newSlave(nil, "b", "p").key("chunks/c1"))
	require.Equal(t, "p/chunks/c1", newSlave(nil, "b", "p/").key("chunks/c1"))
}
