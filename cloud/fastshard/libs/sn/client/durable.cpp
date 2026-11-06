#include "durable.h"

#include <cloud/fastshard/libs/common/context.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/event.h>
#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <util/generic/utility.h>

namespace NCloud::NFastShard {

using silk::FiberFuture;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Suspends the fiber for `delay`, or until the context is cancelled.
void Backoff(TFastShardContext& ctx, TDuration delay)
{
    FiberScheduler::SleepFuture sleep;
    FiberScheduler::sleep(delay.NanoSeconds(), &sleep);

    silk::FiberEvent::Future cancelled;
    ctx.Cancel.wait(&cancelled);

    FiberFuture* futures[] = {&sleep, &cancelled};
    ui64 waitCycles = 0;
    FiberFuture::waitForMultiple(futures, 2, &waitCycles);
    ctx.BackoffCycles += waitCycles;

    int err = 0;
    if (!sleep.isSet(&err)) {
        sleep.cancel();
    }
    Y_UNUSED(sleep.wait());

    cancelled.cancel();
    Y_UNUSED(cancelled.wait());
}

////////////////////////////////////////////////////////////////////////////////

class TDurableClient final: public IStorageNode
{
private:
    const IStorageNodePtr Client;
    const TDurableClientConfig Config;
    const TDurableClientMetricsPtr Metrics;

public:
    TDurableClient(
            IStorageNodePtr client,
            TDurableClientConfig config,
            TDurableClientMetricsPtr metrics)
        : Client(std::move(client))
        , Config(config)
        , Metrics(std::move(metrics))
    {}

#define SN_DURABLE_METHOD(name, ...)                                           \
    NCloud::NProto::T##name##Response name(                                    \
        TFastShardContext& ctx,                                                \
        NCloud::NProto::T##name##Request request) override                     \
    {                                                                          \
        return Call(                                                           \
            &IStorageNode::name,                                               \
            &TDurableClientMetrics::name##Retries,                             \
            ctx,                                                               \
            std::move(request));                                               \
    }                                                                          \
    // SN_DURABLE_METHOD

    SN_METHODS(SN_DURABLE_METHOD)

#undef SN_DURABLE_METHOD

private:
    template <typename TResponse, typename TRequest>
    TResponse Call(
        TResponse (IStorageNode::*method)(TFastShardContext&, TRequest),
        std::atomic<ui64> TDurableClientMetrics::*retries,
        TFastShardContext& ctx,
        TRequest request)
    {
        const TInstant deadline = TInstant::Now() + Config.RetryTimeout;
        TDuration backoff = Config.MinRetryBackoff;

        for (;;) {
            TResponse response = ((*Client).*method)(ctx, request);

            if (GetErrorKind(response.GetError()) !=
                    EErrorKind::ErrorRetriable ||
                ctx.Cancel.isSet() || TInstant::Now() >= deadline)
            {
                return response;
            }

            if (Metrics) {
                ((*Metrics).*retries).fetch_add(1);
            }
            Backoff(ctx, backoff);
            if (ctx.Cancel.isSet()) {
                return response;
            }
            backoff = Min(backoff * 2, Config.MaxRetryBackoff);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageNodePtr CreateDurableClient(
    IStorageNodePtr client,
    TDurableClientConfig config,
    TDurableClientMetricsPtr metrics)
{
    return std::make_shared<TDurableClient>(
        std::move(client),
        config,
        std::move(metrics));
}

}   // namespace NCloud::NFastShard
