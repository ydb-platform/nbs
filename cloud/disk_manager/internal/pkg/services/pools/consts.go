package pools

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
)

////////////////////////////////////////////////////////////////////////////////

const (
	baseDiskBlockSize         = 4096
	defaultBaseDiskBlockCount = storage.DefaultBaseDiskSize / baseDiskBlockSize
)
