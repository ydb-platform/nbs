package backup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestKeys(t *testing.T) {
	require.Equal(
		t,
		"snapshots/disk1/snap1/meta.json",
		SnapshotMetaKey("disk1", "snap1"),
	)
	require.Equal(t, "images/image1/meta.json", ImageMetaKey("image1"))
	require.Equal(t, "chunks/task1.snap1.7", ChunkKey("task1.snap1.7"))
	require.Equal(t, "chunk_maps/snap1", ChunkMapKey("snap1"))
}

func TestFollowerKey(t *testing.T) {
	kek := make([]byte, keySize)

	followerS3, err := NewFollowerS3(nil, "bucket", "p", "kek1", kek)
	require.NoError(t, err)
	require.Equal(
		t,
		"p/images/image1/meta.json",
		followerS3.Key(ImageMetaKey("image1")),
	)

	followerS3, err = NewFollowerS3(nil, "bucket", "", "kek1", kek)
	require.NoError(t, err)
	require.Equal(
		t,
		"images/image1/meta.json",
		followerS3.Key(ImageMetaKey("image1")),
	)
}
