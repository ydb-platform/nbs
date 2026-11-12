#pragma once

#include "host_pool.h"

#include <cloud/blockstore/libs/cells/iface/eds_host_fetcher.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

#include <atomic>
#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

// Asks EDS for the hosts matching one cell's patterns at start and then
// every Interval, give or take half of it, and hands what it lists to the
// pool as configured hosts. A failed fetch, or an empty answer, leaves the
// pool on the previous list. The fetcher is shared and started by its owner.
class TCellHostDiscovery
    : public std::enable_shared_from_this<TCellHostDiscovery>
{
private:
    const TCellHostPoolPtr Pool;
    const IEdsHostFetcherPtr Fetcher;
    const TVector<TString> Patterns;
    const TDuration Interval;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;

    TLog Log;

    std::atomic<bool> Stopped = false;

public:
    TCellHostDiscovery(
        TCellHostPoolPtr pool,
        IEdsHostFetcherPtr fetcher,
        TVector<TString> patterns,
        TDuration interval,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging);

    void Start();
    void Stop();

private:
    void Fetch();
    void OnFetched(const TResultOrError<TVector<TString>>& hosts);
    void ScheduleFetch();
};

using TCellHostDiscoveryPtr = std::shared_ptr<TCellHostDiscovery>;

}   // namespace NCloud::NBlockStore::NCells
