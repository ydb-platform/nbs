package layout

import (
	"testing"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestObjects(t *testing.T) {
	require.Equal(t, "chunks/task1.snap1.7", ChunkObject("task1.snap1.7"))
	require.Equal(t, "snapshots/disk1/snap1/meta.json", MetaObject("disk1", "snap1"))
	require.Equal(t, "snapshots/disk1/snap1/map.bin", MapObject("disk1", "snap1"))
	require.Equal(t, "snapshots/-/snap1/meta.json", MetaObject("", "snap1"))
}

func TestKey(t *testing.T) {
	require.Equal(t, "chunks/c1", Key("", "chunks/c1"))
	require.Equal(t, "p/chunks/c1", Key("p", "chunks/c1"))
	require.Equal(t, "p/chunks/c1", Key("p/", "chunks/c1"))
}
