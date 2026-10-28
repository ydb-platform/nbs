package backup

import (
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"testing"
)

func TestBackupMetadataRejectsUnsupportedKey(t *testing.T) {
	desc := &types.EncryptionDesc{Mode: types.EncryptionMode(999), Key: &types.EncryptionDesc_KmsKey{KmsKey: &types.KmsKey{}}}
	snapshot, err := NewSnapshotMeta(resources.SnapshotMeta{Encryption: desc})
	require.Error(t, err)
	require.Equal(t, SnapshotMeta{}, snapshot)
	image, err := NewImageMeta(resources.ImageMeta{Encryption: desc})
	require.Error(t, err)
	require.Equal(t, ImageMeta{}, image)
}
