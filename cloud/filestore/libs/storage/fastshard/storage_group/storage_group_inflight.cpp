#include "storage_group_inflight.h"

#include <mutex>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

NProto::TError TInflightTracker::Add(TInflightRequest& op)
{
    std::lock_guard guard(Lock);
    if (Stopped) {
        return MakeError(E_REJECTED, "storage group is shutting down");
    }

    ++InflightCount;
    Requests.PushBack(&op);

    return {};
}

void TInflightTracker::Remove(TInflightRequest& op)
{
    std::lock_guard guard(Lock);

    op.Unlink();
    if (--InflightCount == 0 && Stopped) {
        Empty.set();
    }
}

void TInflightTracker::Stop()
{
    {
        std::lock_guard guard(Lock);

        Stopped = true;
        for (auto& op: Requests) {
            op.Ctx.Cancel.set();
        }
        if (InflightCount == 0) {
            Empty.set();
        }
    }

    Empty.wait();
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
