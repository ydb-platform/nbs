package common

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

func TestDiskKindNbs2RoundTrip(t *testing.T) {
	require.Equal(t, "ssd-nbs2", DiskKindToString(types.DiskKind_DISK_KIND_SSD_NBS2))

	kind, err := DiskKindFromString("ssd-nbs2")
	require.NoError(t, err)
	require.Equal(t, types.DiskKind_DISK_KIND_SSD_NBS2, kind)

	require.True(t, IsNbs2DiskKind(types.DiskKind_DISK_KIND_SSD_NBS2))
	require.False(t, IsNbs2DiskKind(types.DiskKind_DISK_KIND_SSD))
	require.True(t, IsNbs2DiskKindString("ssd-nbs2"))
	require.False(t, IsNbs2DiskKindString("ssd"))
	require.False(t, IsLocalDiskKind(types.DiskKind_DISK_KIND_SSD_NBS2))
}
