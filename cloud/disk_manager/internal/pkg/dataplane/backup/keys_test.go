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
		SnapshotMetaKey("", "disk1", "snap1"),
	)
	require.Equal(
		t,
		"p/snapshots/disk1/snap1/meta.json",
		SnapshotMetaKey("p", "disk1", "snap1"),
	)
	require.Equal(
		t,
		"p/images/image1/meta.json",
		ImageMetaKey("p", "image1"),
	)
}
