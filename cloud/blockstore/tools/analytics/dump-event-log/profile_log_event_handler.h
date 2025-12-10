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

struct TInflightCounter
{
    ui32 InflightCount = 0;
    ui64 InflightBytes = 0;

    void Add(ui64 bytes);
};

struct TInflightCounters
{
    TInflightCounter Read;
    TInflightCounter Write;

    void Add(ui32 requestType, ui64 bytes);
};

struct TInflightData
{
    TInflightCounters Disk;
    TInflightCounters HostBlobStorageBased;
    TInflightCounters HostDiskRegistryBased;

    void Add(
        NCloud::NProto::EStorageMediaKind mediaKind,
        ui32 requestType,
        bool sameDisk,
        ui64 bytes);
};

struct TTimeData
{
    // When user sent request
    TInstant StartAt;
    // How long did the throttler postpone the request?
    TDuration Postponed;
    // Net request execution time excluding the troller
    TDuration ExecutionTime;
};

struct IProfileLogEventHandler
{
    virtual ~IProfileLogEventHandler() = default;

    virtual void ProcessRequest(
        const TDiskInfo& diskInfo,
        const TTimeData& timeData,
        ui32 requestType,
        TBlockRange64 blockRange,
        const TReplicaChecksums& replicaChecksums,
        const TInflightData& inflightData) = 0;
};

bool IsReadRequestType(ui32 requestType);
bool IsWriteRequestType(ui32 requestType);

}   // namespace NCloud::NBlockStore
