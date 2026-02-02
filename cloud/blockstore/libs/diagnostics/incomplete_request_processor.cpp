#include "incomplete_request_processor.h"

#include "incomplete_requests.h"
#include "server_stats.h"

#include <cloud/storage/core/libs/diagnostics/stats_handler.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TIncompleteRequestProcessor
    : public NCloud::IStatsHandler
{
    const IServerStatsPtr Stats;
    const TVector<IIncompleteRequestProviderPtr> IncompleteProviders;

    TIncompleteRequestProcessor(
            IServerStatsPtr stats,
            TVector<IIncompleteRequestProviderPtr> incompleteProviders)
        : Stats(std::move(stats))
        , IncompleteProviders(std::move(incompleteProviders))
    {}

    void UpdateStats(bool updateIntervalFinished) override
    {
        TIncompleteRequestsCollector collector = std::bind_front(
            &IServerStats::AddIncompleteRequest,
            Stats.get());

        for (auto& incompleteRequestProvider: IncompleteProviders) {
            incompleteRequestProvider->CollectRequests(collector);
        }

        Stats->UpdateStats(updateIntervalFinished);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TIncompleteRequestProcessorStub
    : public NCloud::IStatsHandler
{
    void UpdateStats(bool updateIntervalFinished) override
    {
        Y_UNUSED(updateIntervalFinished);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NCloud::IStatsHandlerPtr CreateIncompleteRequestProcessor(
    IServerStatsPtr stats,
    TVector<IIncompleteRequestProviderPtr> incompleteProviders)
{
    return std::make_shared<TIncompleteRequestProcessor>(
        std::move(stats),
        std::move(incompleteProviders)
    );
}

NCloud::IStatsHandlerPtr CreateIncompleteRequestProcessorStub()
{
    return std::make_shared<TIncompleteRequestProcessorStub>();
}

}   // namespace NCloud::NBlockStore
