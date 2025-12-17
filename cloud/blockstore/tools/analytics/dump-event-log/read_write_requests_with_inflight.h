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

    void ProcessRequest(
        const TDiskInfo& diskInfo,
        const TTimeData& timeData,
        ui32 requestType,
        TBlockRange64 blockRange,
        const TReplicaChecksums& replicaChecksums,
        const TInflightData& inflightData) override;

    void Finish() override;
};

}   // namespace NCloud::NBlockStore
