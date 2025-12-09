#pragma once

#include <cloud/blockstore/tools/analytics/dump-event-log/profile_log_event_handler.h>

#include <util/stream/file.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TReadWriteRequestsWithInflight: public IProfileLogEventHandler
{
    TFileOutput Output;

public:
    explicit TReadWriteRequestsWithInflight(const TString& filename);
    ~TReadWriteRequestsWithInflight() override;

    void ProcessRequest(
        const TDiskInfo& diskInfo,
        TInstant timestamp,
        ui32 requestType,
        TBlockRange64 blockRange,
        TDuration duration,
        TDuration postponed,
        const TReplicaChecksums& replicaChecksums,
        const TInflightInfo& inflightInfo) override;
};

}   // namespace NCloud::NBlockStore
