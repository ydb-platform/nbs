#pragma once

#include <cloud/fastshard/libs/common/context.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/event.h>
#include <silk/fibers/mutex.h>

#include <util/generic/intrlist.h>
#include <util/system/types.h>

#include <atomic>
#include <mutex>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct TInflightRequest: public TIntrusiveListItem<TInflightRequest>
{
    NCloud::NFastShard::TFastShardContext Ctx;

    explicit TInflightRequest(ui64 requestId)
        : Ctx(requestId)
    {}
};

class TInflightTracker
{
private:
    silk::FiberMutex Lock;
    TIntrusiveList<TInflightRequest> Requests;

    std::atomic<ui32> InflightCount = 0;

    silk::FiberEvent Empty;
    bool Stopped = false;

public:
    NProto::TError Add(TInflightRequest& op)
    {
        std::lock_guard guard(Lock);
        if (Stopped) {
            return MakeError(E_REJECTED, "storage group is shutting down");
        }

        ++InflightCount;
        Requests.PushBack(&op);

        return {};
    }

    void Remove(TInflightRequest& op)
    {
        std::lock_guard guard(Lock);

        op.Unlink();
        if (--InflightCount == 0 && Stopped) {
            Empty.set();
        }
    }

    ui32 GetInflightCount() const
    {
        return InflightCount.load(std::memory_order_relaxed);
    }

    // Cancels the requests in flight, refuses new ones, waits for the rest.
    void Stop()
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
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
