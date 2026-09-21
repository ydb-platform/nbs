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
	require.Equal(
		t,
		"p/images/image1/meta.json",
		NewFollowerS3(nil, "bucket", "p").Key(ImageMetaKey("image1")),
	)
	require.Equal(
		t,
		"images/image1/meta.json",
		NewFollowerS3(nil, "bucket", "").Key(ImageMetaKey("image1")),
	)
}
