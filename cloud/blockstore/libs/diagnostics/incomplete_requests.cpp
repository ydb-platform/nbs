#include "incomplete_requests.h"

#include "server_stats.h"

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TIncompleteRequestProvider
    : public IIncompleteRequestProvider
{
public:
    TIncompleteRequestProvider() = default;

    size_t CollectRequests(
        const TIncompleteRequestsCollector& collector) override
    {
        Y_UNUSED(collector);
        return 0;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IIncompleteRequestProviderPtr CreateIncompleteRequestProviderStub()
{
    return std::make_shared<TIncompleteRequestProvider>();
}

TIncompleteRequestsCollector CreateIncompleteRequestsCollectorStub()
{
    return [](TCallContext& callContext,
              const TMetricRequest& metricRequest,
              TRequestTime time)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(metricRequest);
        Y_UNUSED(time);
    };
}

}   // namespace NCloud::NBlockStore
