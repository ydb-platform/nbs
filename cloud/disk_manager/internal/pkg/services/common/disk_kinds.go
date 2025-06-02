package common

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

func DiskKindToString(kind types.DiskKind) string {
	switch kind {
	case types.DiskKind_DISK_KIND_SSD:
		return "ssd"
	case types.DiskKind_DISK_KIND_HDD:
		return "hdd"
	case types.DiskKind_DISK_KIND_SSD_NONREPLICATED:
		return "ssd-nonreplicated"
	case types.DiskKind_DISK_KIND_SSD_MIRROR2:
		return "ssd-mirror2"
	case types.DiskKind_DISK_KIND_SSD_MIRROR3:
		return "ssd-mirror3"
	case types.DiskKind_DISK_KIND_SSD_LOCAL:
		return "ssd-local"
	case types.DiskKind_DISK_KIND_HDD_NONREPLICATED:
		return "hdd-nonreplicated"
	case types.DiskKind_DISK_KIND_HDD_LOCAL:
		return "hdd-local"
	}
	return "unknown"
}

func DiskKindFromString(kind string) (types.DiskKind, error) {
	switch kind {
	case "ssd":
		return types.DiskKind_DISK_KIND_SSD, nil
	case "hdd":
		return types.DiskKind_DISK_KIND_HDD, nil
	case "ssd-nonreplicated":
		return types.DiskKind_DISK_KIND_SSD_NONREPLICATED, nil
	case "ssd-mirror2":
		return types.DiskKind_DISK_KIND_SSD_MIRROR2, nil
	case "ssd-mirror3":
		return types.DiskKind_DISK_KIND_SSD_MIRROR3, nil
	case "ssd-local":
		return types.DiskKind_DISK_KIND_SSD_LOCAL, nil
	case "hdd-nonreplicated":
		return types.DiskKind_DISK_KIND_HDD_NONREPLICATED, nil
	case "hdd-local":
		return types.DiskKind_DISK_KIND_HDD_LOCAL, nil
	default:
		return 0, errors.NewInvalidArgumentError(
			"unknown disk kind %v",
			kind,
		)
	}
}
