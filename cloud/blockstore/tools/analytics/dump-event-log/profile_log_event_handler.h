#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/tools/analytics/libs/event-log/dump.h>

#include <util/datetime/base.h>
#include <util/generic/map.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IProfileLogEventHandler
{
    using TReplicaChecksums =
        google::protobuf::RepeatedPtrField<NProto::TReplicaChecksum>;

    virtual ~IProfileLogEventHandler() = default;

    virtual void ProcessRequest(
        const TString& diskId,
        TInstant timestamp,
        ui32 requestType,
        TBlockRange64 blockRange,
        TDuration duration,
        const TReplicaChecksums& replicaChecksums) = 0;
};

}   // namespace NCloud::NBlockStore
