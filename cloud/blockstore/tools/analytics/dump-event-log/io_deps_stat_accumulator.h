#pragma once

#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/tools/analytics/dump-event-log/profile_log_event_handler.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TIoDepsStatAccumulator
{
public:
    struct TRequestInfo
    {
        const TDiskInfo& DiskInfo;
        const TInstant StartAt;
        const TBlockRange64 BlockRange;
        const TDuration Duration;
        const TDuration Postponed;
        const ui32 RequestType;
        const TReplicaChecksums ReplicaChecksums;
        TInflightInfo InflightInfo;

        TRequestInfo(
            const TDiskInfo& diskInfo,
            TInstant startAt,
            TDuration duration,
            TDuration postponed,
            ui32 requestType,
            TBlockRange64 blockRange,
            const TReplicaChecksums& replicaChecksums);

        [[nodiscard]] TInstant ExecutionStartAt() const
        {
            return StartAt + Postponed;
        }
        [[nodiscard]] TInstant FinishedAt() const
        {
            return StartAt + Postponed + Duration;
        }
    };

private:
    THashMap<TString, TDiskInfo> DiskInfos;
    TMultiMap<TInstant, TRequestInfo> Requests;
    TVector<std::unique_ptr<IProfileLogEventHandler>> EventHandlers;

public:
    explicit TIoDepsStatAccumulator(const TString& knownDisksFile);
    virtual ~TIoDepsStatAccumulator();

    void AddEventHandler(std::unique_ptr<IProfileLogEventHandler> handler);

    void ProcessRequest(
        const TString& diskId,
        TInstant timestamp,
        ui32 requestType,
        TBlockRange64 blockRange,
        TDuration duration,
        TDuration postponed,
        const TReplicaChecksums& replicaChecksums);

    [[nodiscard]] bool HasEventHandlers() const
    {
        return !EventHandlers.empty();
    }

private:
    const TDiskInfo& GetDiskInfo(const TString& diskId);
    void ExtractRequests(TInstant windowStart);
};

}   // namespace NCloud::NBlockStore
