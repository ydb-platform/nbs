#pragma once

#include "public.h"

#include "config.h"

#include <cloud/fastshard/libs/sn/client/durable.h>
#include <cloud/fastshard/libs/sn/iface/storage_node.h>
#include <cloud/filestore/tools/testing/loadtest/protos/loadtest.pb.h>

namespace NCloud::NFastShard::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

/**
 * One load run against one device: IoDepth fibers, each issuing
 * WriteLogRecord / ReadPages requests back to back until the duration
 * elapses, the request count is reached or the app context asks to stop.
 *
 * Written records form a single chain (every record's PrevLsn is the
 * previous record's Lsn) that continues from whatever the device's
 * journal already holds, so the run is valid against a used device.
 */
struct ILoadTest
{
    virtual ~ILoadTest() = default;

    /**
     * Runs the load from the calling (non-fiber) thread and blocks until
     * it completes. FiberScheduler must be initialized by the caller.
     *
     * @return - Per-action counts and latencies; Success is false if any
     *           request failed or the run could not be set up.
     */
    virtual NFileStore::NProto::TTestStats Run() = 0;
};

/**
 * @param ctx - Tells the test when to stop (ShouldStop). Must outlive the
 *              test.
 * @param config - Validated parameters of the test.
 * @param client - Durable client to the storage node under test.
 * @param clientMetrics - Retry counters of `client`; the test reports
 *                        them in its stats.
 */
ILoadTestPtr CreateLoadTest(
    const TAppContext& ctx,
    TLoadTestConfig config,
    IStorageNodePtr client,
    TDurableClientMetricsPtr clientMetrics);

}   // namespace NCloud::NFastShard::NLoadTest
