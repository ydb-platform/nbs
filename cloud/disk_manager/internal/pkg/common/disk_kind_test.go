package common

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

func TestDiskKindSsdDirectMirror3Of5GroupRoundTrip(t *testing.T) {
	require.Equal(t, "ssd-direct-mirror3of5-group", DiskKindToString(types.DiskKind_DISK_KIND_SSD_DIRECT_MIRROR3OF5_GROUP))

	kind, err := DiskKindFromString("ssd-direct-mirror3of5-group")
	require.NoError(t, err)
	require.Equal(t, types.DiskKind_DISK_KIND_SSD_DIRECT_MIRROR3OF5_GROUP, kind)

	require.True(t, IsSsdDirectMirror3Of5GroupDiskKind(types.DiskKind_DISK_KIND_SSD_DIRECT_MIRROR3OF5_GROUP))
	require.False(t, IsSsdDirectMirror3Of5GroupDiskKind(types.DiskKind_DISK_KIND_SSD))
	require.False(t, IsLocalDiskKind(types.DiskKind_DISK_KIND_SSD_DIRECT_MIRROR3OF5_GROUP))
}
