package driver

import (
	"errors"

	"github.com/container-storage-interface/spec/lib/go/csi"
	nbsapi "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbsclient "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	nfsclient "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	storagecoreapi "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func hasReadOnlyVolumeAccess(
	accessMode *csi.VolumeCapability_AccessMode,
	readonly bool,
) bool {
	if accessMode != nil {
		switch accessMode.GetMode() {
		case csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
			csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY:
			return true
		}
	}

	return readonly
}

func getNbsVolumeAccessMode(
	accessMode *csi.VolumeCapability_AccessMode,
	readonly bool,
) nbsapi.EVolumeAccessMode {
	if hasReadOnlyVolumeAccess(accessMode, readonly) {
		return nbsapi.EVolumeAccessMode_VOLUME_ACCESS_USER_READ_ONLY
	}

	return nbsapi.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE
}

func getNbsVolumeMountMode(
	accessMode *csi.VolumeCapability_AccessMode,
) nbsapi.EVolumeMountMode {
	if accessMode != nil {
		switch accessMode.GetMode() {
		case csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY:
			return nbsapi.EVolumeMountMode_VOLUME_MOUNT_REMOTE
		}
	}

	return nbsapi.EVolumeMountMode_VOLUME_MOUNT_LOCAL
}

func getNbsErrorCode(err error) (uint32, bool) {
	if err == nil {
		return 0, false
	}

	var nbsClientErr *nbsclient.ClientError
	if errors.As(err, &nbsClientErr) {
		return nbsClientErr.Code, true
	}

	var nfsClientErr *nfsclient.ClientError
	if errors.As(err, &nfsClientErr) {
		return nfsClientErr.Code, true
	}

	return 0, false
}

func getGrpcErrorCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}

	errorCode, ok := getNbsErrorCode(err)
	if ok {
		switch errorCode {
		case nbsclient.E_INVALID_SESSION, nbsclient.E_MOUNT_CONFLICT:
			return codes.Unavailable
		case nbsclient.E_GRPC_CANCELLED, nfsclient.E_GRPC_CANCELLED:
			return codes.Canceled
		case nbsclient.E_GRPC_UNAVAILABLE, nfsclient.E_GRPC_UNAVAILABLE:
			return codes.Unavailable
		case nbsclient.E_GRPC_DEADLINE_EXCEEDED, nfsclient.E_GRPC_DEADLINE_EXCEEDED:
			return codes.DeadlineExceeded
		case nbsclient.E_REJECTED, nfsclient.E_REJECTED:
			return codes.Unavailable
		}
	}

	status, ok := status.FromError(err)
	if !ok {
		return codes.Internal
	}

	return status.Code()
}
