#include "host_discovery.h"

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <util/random/random.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TCellHostDiscovery::TCellHostDiscovery(
        TCellHostPoolPtr pool,
        IEdsHostFetcherPtr fetcher,
        TVector<TString> patterns,
        TDuration interval,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging)
    : Pool(std::move(pool))
    , Fetcher(std::move(fetcher))
    , Patterns(std::move(patterns))
    , Interval(interval)
    , Timer(std::move(timer))
    , Scheduler(std::move(scheduler))
{
    if (logging) {
        Log = logging->CreateLog("BLOCKSTORE_CELLS");
    }
}

void TCellHostDiscovery::Start()
{
    Fetch();
}

void TCellHostDiscovery::Stop()
{
    Stopped = true;
}

void TCellHostDiscovery::Fetch()
{
    if (Stopped) {
        return;
    }

    TFuture<TResultOrError<TVector<TString>>> future;
    try {
        future = Fetcher->FetchHosts(Patterns);
    } catch (...) {
        OnFetched(MakeError(E_FAIL, CurrentExceptionMessage()));
        return;
    }

    future.Subscribe(
        [weak = weak_from_this()](const auto& future)
        {
            auto self = weak.lock();
            if (!self) {
                return;
            }

            try {
                self->OnFetched(future.GetValue());
            } catch (...) {
                self->OnFetched(
                    MakeError(E_FAIL, CurrentExceptionMessage()));
            }
        });
}

void TCellHostDiscovery::OnFetched(
    const TResultOrError<TVector<TString>>& hosts)
{
    if (HasError(hosts)) {
        STORAGE_WARN(
            "Host discovery for cell " << Pool->GetCellId()
                << " failed: " << FormatError(hosts.GetError()));
    } else if (hosts.GetResult().empty()) {
        // a cell is never empty; believing this would tear down every host
        // learned so far over what is most likely a gap on the EDS side
        STORAGE_WARN(
            "Host discovery for cell " << Pool->GetCellId()
                << " listed no hosts, keeping the previous ones");
    } else {
        Pool->UpdateConfiguredHosts(hosts.GetResult());
    }

    ScheduleFetch();
}

void TCellHostDiscovery::ScheduleFetch()
{
    if (Stopped) {
        return;
    }

    // nodes restarted together would otherwise ask EDS in lockstep for good
    const auto delay = Interval / 2 + TDuration::MicroSeconds(
        RandomNumber<ui64>(Interval.MicroSeconds() + 1));

    Scheduler->Schedule(
        Timer->Now() + delay,
        [weak = weak_from_this()]
        {
            if (auto self = weak.lock()) {
                self->Fetch();
            }
        });
}

}   // namespace NCloud::NBlockStore::NCells
