#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>

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

    auto now = GetCycleCount();
    for (auto it = InFlightRequests.begin(); it != InFlightRequests.end(); ) {
        const auto& request = it->second;
        if (!request.IsCompleted()) {
            StatsRegistry->AddIncompleteRequest(request.ToIncompleteRequest(now));
            ++it;
        } else {
            InFlightRequests.erase(it++);
        }
    }

    if (StatsFetcher) {
        auto now = ctx.Now();

        auto interval = (now - LastCpuWaitQuery).MicroSeconds();
        auto [cpuWait, error] = StatsFetcher->GetCpuWait();
        if (HasError(error)) {
        auto errorMessage =
            ReportCpuWaitCounterReadError(error.GetMessage());
            LOG_WARN_S(
                ctx,
                TFileStoreComponents::SERVICE,
                "Failed to get CpuWait stats: " << errorMessage);
        }

        auto cpuLack = 100 * cpuWait.MicroSeconds();
        cpuLack /= interval;
        *CpuWait = cpuLack;

        LastCpuWaitQuery = now;

        if (cpuLack >= StorageConfig->GetCpuLackThreshold()) {
            LOG_WARN_S(
                ctx,
                TFileStoreComponents::SERVICE,
                "Cpu wait is " << cpuLack);
        }
    }

    StatsRegistry->UpdateStats(true);
    ScheduleUpdateStats(ctx);
}

} // namespace NCloud::NFileStore::NStorage
