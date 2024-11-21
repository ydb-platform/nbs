#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>

#include <cloud/storage/core/libs/diagnostics/cgroup_stats_fetcher.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 CpuLackPercentsMultiplier = 100;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleUpdateStats(
    const TEvServicePrivate::TEvUpdateStats::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

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
    if (CgroupStatsFetcher && CpuWait) {
        auto now = ctx.Now();

        auto interval = (now - LastCpuWaitQuery).MicroSeconds();
        if (auto [cpuWait, error] = CgroupStatsFetcher->GetCpuWait();
            !HasError(error))
        {
            auto cpuWaitValue = cpuWait.MicroSeconds();
            auto cpuLack = CpuLackPercentsMultiplier * cpuWaitValue / interval;

            LOG_DEBUG_S(
                ctx,
                TFileStoreComponents::SERVICE,
                "CpuWait stats: lack = " << cpuLack
                                         << "; interval = " << interval
                                         << "; wait = " << cpuWaitValue);

            *CpuWait = cpuLack;
            LastCpuWaitQuery = now;

            if (cpuLack >= StorageConfig->GetCpuLackThreshold()) {
                LOG_WARN_S(
                    ctx,
                    TFileStoreComponents::SERVICE,
                    "Cpu wait is " << cpuLack);
            }
        } else {
            LOG_ERROR_S(
                ctx,
                TFileStoreComponents::SERVICE,
                "Failed to get CpuWait stats: " << error);
        }
    }

    StatsRegistry->UpdateStats(true);
    ScheduleUpdateStats(ctx);
}

} // namespace NCloud::NFileStore::NStorage
