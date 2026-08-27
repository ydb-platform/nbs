#pragma once

#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>

#include <util/generic/string.h>
#include <util/system/types.h>

#include <atomic>
#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * Connection pool metrics of a storage node client. The client updates
 * the counters; the owner reads them at any time (all fields are
 * atomics, values are monotonically increasing).
 */
struct TStorageNodeClientMetrics
{
    // TCP connections successfully established.
    std::atomic<ui64> ConnectionsCreated{0};

    // Distinct connections that completed at least one request.
    std::atomic<ui64> ConnectionsUsed{0};

    // Requests that completed without a transport error.
    std::atomic<ui64> RequestsCompleted{0};
};

using TStorageNodeClientMetricsPtr =
    std::shared_ptr<TStorageNodeClientMetrics>;

/**
 * Returns an IStorageNode that forwards every call over TCP to an sn
 * server at `host:port`. Each call is wrapped in a
 * TDeviceProtocolRequest, sent length-prefixed, and its
 * TDeviceProtocolResponse is unpacked into the matching concrete
 * response.
 *
 * Concurrency: methods on the returned client may be called from any
 * silk fiber. Connections come from a pool: each call takes an idle
 * connection (or opens a new one) for the duration of its round trip
 * and returns it afterwards, so concurrent calls proceed in parallel.
 * The pool grows to the maximum number of concurrent calls observed.
 * The client must not be destroyed while a call is in flight.
 *
 * Errors:
 *   - I/O failures (connect / send / recv / parse) surface as a
 *     response whose Error field is set to E_REJECTED. The failed
 *     connection is closed and dropped from the pool; the next call
 *     opens a fresh one.
 *   - A TDeviceProtocolResponse.ProtocolError from the server is
 *     copied into the concrete response's Error field.
 *
 * Must be called from a silk fiber context.
 *
 * @param host - Server hostname or IP.
 * @param port - Server TCP port.
 *
 * @return - Shared owner of the client instance.
 */
IStorageNodePtr CreateStorageNodeClient(TString host, ui16 port);

/**
 * Same as above, with connection pool metrics reported into `metrics`.
 *
 * @param host - Server hostname or IP.
 * @param port - Server TCP port.
 * @param metrics - (out) Counters updated by the client; may be null.
 *
 * @return - Shared owner of the client instance.
 */
IStorageNodePtr CreateStorageNodeClient(
    TString host,
    ui16 port,
    TStorageNodeClientMetricsPtr metrics);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
