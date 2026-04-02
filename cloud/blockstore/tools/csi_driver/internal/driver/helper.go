package driver

import (
	storagecoreapi "github.com/ydb-platform/nbs/cloud/storage/core/protos"
)

func getStorageMediaKind(parameters map[string]string) storagecoreapi.EStorageMediaKind {
	kind, ok := parameters["storage-media-kind"]
	if ok {
		switch kind {
		case "hdd":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_HDD
		case "hybrid":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_HDD
		case "ssd":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD
		case "ssd_nonrepl":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD_NONREPLICATED
		case "ssd_mirror2":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD_MIRROR2
		case "ssd_mirror3":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD_MIRROR3
		case "ssd_local":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD_LOCAL
		case "hdd_local":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_HDD_LOCAL
		case "hdd_nonrepl":
			return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_HDD_NONREPLICATED
		}
	}

	return storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD
}

func isDiskRegistryMediaKind(mediaKind storagecoreapi.EStorageMediaKind) bool {
	switch mediaKind {
	case storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD_NONREPLICATED,
		storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD_MIRROR2,
		storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD_MIRROR3,
		storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_SSD_LOCAL,
		storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_HDD_LOCAL,
		storagecoreapi.EStorageMediaKind_STORAGE_MEDIA_HDD_NONREPLICATED:
		return true
	default:
		return false
	}
}
