#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <cloud/storage/core/protos/media.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TMetricRequest
{
    const EBlockStoreRequest RequestType;
    TString ClientId;
    TString DiskId;
    TString Peer;
    IVolumeInfoPtr VolumeInfo;
    ui64 StartIndex = 0;
    NCloud::NProto::EStorageMediaKind MediaKind =
        NCloud::NProto::STORAGE_MEDIA_HDD;
    ui64 RequestBytes = 0;
    TInstant RequestTimestamp;
    bool Unaligned = false;
    bool CellRequest = false;
    NProto::EVolumeAccessMode AccessMode =
        NProto::EVolumeAccessMode::VOLUME_ACCESS_READ_WRITE;
    NProto::EVolumeMountMode MountMode =
        NProto::EVolumeMountMode::VOLUME_MOUNT_LOCAL;

    TMetricRequest(EBlockStoreRequest requestType)
        : RequestType(requestType)
    {}
};

}   // namespace NCloud::NBlockStore
