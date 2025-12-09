#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/tools/analytics/libs/event-log/dump.h>

#include <cloud/storage/core/libs/common/media.h>

#include <util/datetime/base.h>
#include <util/generic/map.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

using TReplicaChecksums =
    google::protobuf::RepeatedPtrField<NProto::TReplicaChecksum>;

struct TDiskInfo
{
    const TString DiskId;
    NCloud::NProto::EStorageMediaKind MediaKind =
        NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT;
    ui32 BlockSize = 0;
};

struct TInflightInfo
{
    ui32 DiskReadInflight = 0;
    ui32 DiskReadInflightByteCount = 0;
    ui32 DiskWriteInflight = 0;
    ui32 DiskWriteInflightByteCount = 0;

    ui32 HostBlobStorageReadInflight = 0;
    ui32 HostDiskRegistryReadInflight = 0;
    ui32 HostBlobStorageReadInflightByteCount = 0;
    ui32 HostDiskRegistryReadInflightByteCount = 0;

    ui32 HostBlobStorageWriteInflight = 0;
    ui32 HostDiskRegistryWriteInflight = 0;
    ui32 HostBlobStorageWriteInflightByteCount = 0;
    ui32 HostDiskRegistryWriteInflightByteCount = 0;
};

struct IProfileLogEventHandler
{
    virtual ~IProfileLogEventHandler() = default;

    virtual void ProcessRequest(
        const TDiskInfo& diskInfo,
        TInstant timestamp,
        ui32 requestType,
        TBlockRange64 blockRange,
        TDuration duration,
        TDuration postponed,
        const TReplicaChecksums& replicaChecksums,
        const TInflightInfo& inflightInfo) = 0;
};

bool IsReadRequestType(ui32 requestType);
bool IsWriteRequestType(ui32 requestType);

}   // namespace NCloud::NBlockStore
