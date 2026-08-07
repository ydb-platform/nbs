#include "executor.h"

#include "vhost.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>

#include <cloud/storage/core/libs/common/thread.h>

namespace NCloud::NBlockStore::NVhost {

////////////////////////////////////////////////////////////////////////////////

TExecutor::TExecutor(
        TString name,
        IServerStats& serverStats,
        IVhostQueuePtr vhostQueue,
        const TAffinity& affinity)
    : Name(std::move(name))
    , ExecutorScope(serverStats.StartExecutor())
    , VhostQueue(std::move(vhostQueue))
    , Affinity(affinity)
{}

void TExecutor::Shutdown()
{
    VhostQueue->Stop();
    Join();
}

void* TExecutor::ThreadProc()
{
    TAffinityGuard affinityGuard(Affinity);

    ::NCloud::SetCurrentThreadName(Name);

    while (true) {
        int res = RunRequestQueue();
        if (res != -EAGAIN) {
            if (res < 0) {
                ReportVhostQueueRunningError({{"return_code", -res}});
            }
            break;
        }

        while (auto req = VhostQueue->DequeueRequest()) {
            ProcessRequest(std::move(req));
        }
    }

    return nullptr;
}

int TExecutor::RunRequestQueue()
{
    auto activity = ExecutorScope.StartWait();

    return VhostQueue->Run();
}

void TExecutor::ProcessRequest(TVhostRequestPtr vhostRequest)
{
    auto activity = ExecutorScope.StartExecute();

    auto* processor = static_cast<IRequestProcessor*>(vhostRequest->Cookie);
    Y_ABORT_UNLESS(processor);
    processor->ProcessRequest(std::move(vhostRequest));
}

}   // namespace NCloud::NBlockStore::NVhost
