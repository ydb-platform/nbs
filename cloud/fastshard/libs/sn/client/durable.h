#pragma once

#include <cloud/fastshard/libs/sn/iface/storage_node.h>

#include <util/datetime/base.h>
#include <util/system/types.h>

#include <atomic>
#include <memory>

namespace NCloud::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct TDurableClientConfig
{
    // A request whose attempts have been failing for this long is given up;
    // zero disables retries.
    TDuration RetryTimeout = TDuration::Seconds(30);

    // Pause before a repeated attempt: doubles from the first to the second
    // value with every failure.
    TDuration MinRetryBackoff = TDuration::MilliSeconds(1);
    TDuration MaxRetryBackoff = TDuration::MilliSeconds(100);
};

/**
 * Retry counters of a durable client, one per IStorageNode method
 * (WriteLogRecordRetries, ...). The client updates them; the owner reads
 * them at any time.
 */
struct TDurableClientMetrics
{
    // Attempts of the method repeated after a retriable error.
#define SN_DURABLE_METRIC(name, ...)                                           \
    std::atomic<ui64> name##Retries{0};                                        \
    // SN_DURABLE_METRIC

    SN_METHODS(SN_DURABLE_METRIC)

#undef SN_DURABLE_METRIC
};

using TDurableClientMetricsPtr = std::shared_ptr<TDurableClientMetrics>;

/**
 * Returns an IStorageNode that forwards every call to `client` and repeats
 * it, with exponential backoff, while the response carries a retriable
 * error (GetErrorKind == ErrorRetriable, e.g. E_REJECTED or E_TIMEOUT).
 * The last response is returned as is once the error is not retriable, the
 * attempts have been failing for RetryTimeout, or the context is cancelled.
 * A cancel also ends a backoff pause early. Backoff time is accounted in
 * the context's BackoffCycles.
 *
 * Every attempt sends a copy of the original request, so the callee may
 * see one request more than once.
 *
 * Must be called from a silk fiber context, like the wrapped client.
 *
 * @param client - Storage node every attempt is sent to.
 * @param config - Retry limits.
 * @param metrics - (out) Counters updated by the client; may be null.
 *
 * @return - Shared owner of the client instance.
 */
IStorageNodePtr CreateDurableClient(
    IStorageNodePtr client,
    TDurableClientConfig config,
    TDurableClientMetricsPtr metrics);

}   // namespace NCloud::NFastShard
