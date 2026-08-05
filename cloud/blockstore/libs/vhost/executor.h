#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

#include <cloud/storage/core/libs/common/affinity.h>
#include <cloud/storage/core/libs/diagnostics/executor_counters.h>

#include <util/generic/string.h>
#include <util/system/thread.h>

#include <memory>

namespace NCloud::NBlockStore::NVhost {

////////////////////////////////////////////////////////////////////////////////

// Implemented by TEndpoint. A pointer to it is used as the vhost device
// cookie, so that an executor can dispatch a request dequeued from its request
// queue to the endpoint the request belongs to.
struct IRequestProcessor
{
    virtual ~IRequestProcessor() = default;

    virtual void ProcessRequest(TVhostRequestPtr vhostRequest) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Owns a single vhost request queue and the thread that runs it. The queue is
// shared by all endpoints assigned to this executor.
class TExecutor final
    : public ISimpleThread
{
private:
    const TString Name;
    TExecutorCounters::TExecutorScope ExecutorScope;
    const IVhostQueuePtr VhostQueue;
    TAffinity Affinity;

public:
    TExecutor(
        TString name,
        IServerStats& serverStats,
        IVhostQueuePtr vhostQueue,
        const TAffinity& affinity);

    void Shutdown();

    const IVhostQueuePtr& GetQueue() const
    {
        return VhostQueue;
    }

private:
    void* ThreadProc() override;

    int RunRequestQueue();

    void ProcessRequest(TVhostRequestPtr vhostRequest);
};

using TExecutorPtr = std::unique_ptr<TExecutor>;

}   // namespace NCloud::NBlockStore::NVhost
