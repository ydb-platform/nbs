#pragma once

#include "options.h"

#include <cloud/fastshard/sn/iface/storage_node.h>
#include <cloud/filestore/tools/testing/loadtest/protos/loadtest.pb.h>

#include <memory>

namespace NCloud::NFastShard::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

/**
 * One load run against one device: IoDepth fibers, each issuing
 * WriteLogRecord / ReadPages requests back to back until the duration
 * elapses, the request count is reached or Stop is called.
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

    // Makes the running fibers finish their current request and exit.
    // Safe to call from a signal handler or another thread.
    virtual void Stop() = 0;
};

using ILoadTestPtr = std::shared_ptr<ILoadTest>;

/**
 * @param options - Validated options.
 * @param client - Preset by tests; otherwise a TCP client to Host:Port
 *                 is created inside the driver fiber.
 */
ILoadTestPtr CreateLoadTest(TOptions options, IStorageNodePtr client = {});

}   // namespace NCloud::NFastShard::NLoadTest
