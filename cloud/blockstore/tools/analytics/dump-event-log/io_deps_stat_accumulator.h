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
        TDiskInfo const* DiskInfo = nullptr;
        TTimeData TimeData;
        TBlockRange64 BlockRange;
        ui32 RequestType;
        TReplicaChecksums ReplicaChecksums;
        TInflightData InflightData;

        TRequestInfo(
            TDiskInfo const* diskInfo,
            TInstant startAt,
            TDuration duration,
            TDuration postponed,
            ui32 requestType,
            TBlockRange64 blockRange,
            const TReplicaChecksums& replicaChecksums);
    };

private:
    const THashMap<TString, TDiskInfo> KnownDiskInfos;
    TVector<std::unique_ptr<IProfileLogEventHandler>> EventHandlers;

    THashMap<TString, TDiskInfo> DiskInfos;
    TVector<TRequestInfo> Requests;

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
    TDiskInfo& GetDiskInfo(const TString& diskId);
    void ProcessRequests();
    void FinishEventHandler();
};

}   // namespace NCloud::NBlockStore
