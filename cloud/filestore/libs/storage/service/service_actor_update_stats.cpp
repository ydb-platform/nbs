#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>
#include <cloud/filestore/libs/storage/core/system_counters.h>

#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/stats_fetcher.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleUpdateStats(
    const TEvServicePrivate::TEvUpdateStats::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (State) {
        i64 totalFileSystems = 0;
        i64 totalTablets = 0;
        i64 hddFileSystems = 0;
        i64 ssdFileSystems = 0;
        i64 hddTablets = 0;
        i64 ssdTablets = 0;
        for (const auto& item: State->GetLocalFileStores()) {
            constexpr auto MediaSsd = NProto::EStorageMediaKind::STORAGE_MEDIA_SSD;
            if (!item.second.IsShard) {
                auto& counter =
                    item.second.Config.GetStorageMediaKind() == MediaSsd ?
                        ssdFileSystems:
                        hddFileSystems;
                ++counter;
                ++totalFileSystems;
            }
            auto& counter =
                item.second.Config.GetStorageMediaKind() == MediaSsd ?
                    ssdTablets:
                    hddTablets;
            ++counter;
            ++totalTablets;

        }
        TotalFileSystemCount->Set(totalFileSystems);
        TotalTabletCount->Set(totalTablets);
        SsdFileSystemCount->Set(ssdFileSystems);
        HddFileSystemCount->Set(hddFileSystems);
        SsdTabletCount->Set(ssdTablets);
        HddTabletCount->Set(hddTablets);
    }

    {
        const ui64 nowCycles = GetCycleCount();
        const auto keys = InFlightRequests->GetKeys();
        InFlightRequestCount->Set(keys.size());
        for (const ui64 key: keys) {
            std::unique_ptr<TIncompleteRequest> incompleteRequest;

            {
                auto lockedRequest = InFlightRequests->FindAndLock(key);
                auto* request = lockedRequest.Get();
                if (request && !request->IsCompleted()) {
                    incompleteRequest = request->ToIncompleteRequest(nowCycles);
                }
            }

            if (incompleteRequest) {
                StatsRegistry->AddIncompleteRequest(*incompleteRequest);
            }
        }
    }

    {
        const auto stats = InFlightRequests->GetStats();
        CompletedRequestCountWithLogData->Set(
            stats.CompletedRequestCountWithLogData);
        CompletedRequestCountWithError->Set(
            stats.CompletedRequestCountWithError);
        CompletedRequestCountWithoutErrorOrLogData->Set(
            stats.CompletedRequestCountWithoutErrorOrLogData);
    }

    if (StatsFetcher) {
        auto [cpuWait, error] = StatsFetcher->GetCpuWait();
        auto now = ctx.Monotonic();
        if (HasError(error)) {
            auto errorMessage =
                ReportCpuWaitCounterReadError(error.GetMessage());
            LOG_WARN_S(
                ctx,
                TFileStoreComponents::SERVICE,
                "Failed to get CpuWait stats: " << errorMessage);
        } else if (LastCpuWaitTs < now) {
            auto intervalUs = (now - LastCpuWaitTs).MicroSeconds();
            auto cpuLack = 100 * cpuWait.MicroSeconds();
            cpuLack /= intervalUs;
            *CpuWaitCounter = cpuLack;
            SystemCounters->CpuLack.store(cpuLack, std::memory_order_relaxed);

            if (cpuLack >= StorageConfig->GetCpuLackThreshold()) {
                LOG_WARN_S(
                    ctx,
                    TFileStoreComponents::SERVICE,
                    "Cpu wait is " << cpuLack);
            }

            LastCpuWaitTs = now;
        }
    }

    StatsRegistry->UpdateStats(true);
    ScheduleUpdateStats(ctx);
}

} // namespace NCloud::NFileStore::NStorage
