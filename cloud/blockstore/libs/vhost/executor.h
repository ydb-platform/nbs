#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

#include <cloud/storage/core/libs/common/affinity.h>
#include <cloud/storage/core/libs/diagnostics/executor_counters.h>

#include <util/generic/string.h>
#include <util/system/thread.h>

#include <atomic>
#include <memory>

namespace NCloud::NBlockStore::NVhost {

////////////////////////////////////////////////////////////////////////////////

// Implemented by TEndpoint. A pointer to it is used as the vhost device
// cookie, so that an executor can dispatch a request dequeued from a shared
// request queue to the endpoint the request belongs to.
struct IRequestProcessor
{
    virtual ~IRequestProcessor() = default;

    virtual void ProcessRequest(TVhostRequestPtr vhostRequest) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Owns a single vhost request queue and the thread that runs it. A queue is
// shared by all endpoints assigned to this executor, and a single endpoint may
// be assigned to several executors at once - see TServer::PickExecutors.
class TExecutor final: public ISimpleThread
{
private:
    const TString Name;
    TExecutorCounters::TExecutorScope ExecutorScope;
    const IVhostQueuePtr VhostQueue;
    TAffinity Affinity;

    // Number of vhost queues currently assigned to this executor. Maintained
    // by TEndpoint's ctor/dtor and used for load-balanced executor selection.
    std::atomic<ui32> AssignedVhostQueuesCount = 0;

public:
    TExecutor(
        TString name,
        IServerStats& serverStats,
        IVhostQueuePtr vhostQueue,
        TAffinity affinity);

    void Shutdown();

    const IVhostQueuePtr& GetQueue() const
    {
        return VhostQueue;
    }

    void OnVhostQueuesAssigned(ui32 count)
    {
        AssignedVhostQueuesCount.fetch_add(count, std::memory_order_relaxed);
    }

    void OnVhostQueuesReleased(ui32 count)
    {
        AssignedVhostQueuesCount.fetch_sub(count, std::memory_order_relaxed);
    }

    ui32 GetAssignedVhostQueuesCount() const
    {
        return AssignedVhostQueuesCount.load(std::memory_order_relaxed);
    }

private:
    void* ThreadProc() override;

    int RunRequestQueue();

    void ProcessRequest(TVhostRequestPtr vhostRequest);
};

using TExecutorPtr = std::unique_ptr<TExecutor>;

}   // namespace NCloud::NBlockStore::NVhost
