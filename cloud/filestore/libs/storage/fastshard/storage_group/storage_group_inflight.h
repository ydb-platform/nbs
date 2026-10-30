#pragma once

#include <cloud/fastshard/libs/common/context.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/public.h>

#include <silk/fibers/event.h>
#include <silk/fibers/mutex.h>

#include <util/generic/intrlist.h>
#include <util/system/types.h>

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
    const ui32 Limit = 0;

    Y_CACHE_ALIGNED silk::FiberMutex Lock;
    TIntrusiveList<TInflightRequest> Requests;
    ui32 InflightCount = 0;

    silk::FiberEvent Empty;
    bool Stopped = false;

public:
    explicit TInflightTracker(ui32 limit)
        : Limit(limit)
    {
        Empty.set();
    }

    NProto::TError Add(TInflightRequest& op)
    {
        std::lock_guard guard(Lock);
        if (Stopped) {
            return MakeError(E_REJECTED, "storage group is shutting down");
        }
        if (Limit && InflightCount >= Limit) {
            return MakeError(E_REJECTED, "storage group is busy");
        }

        ++InflightCount;
        Requests.PushBack(&op);
        Empty.reset();

        return {};
    }

    NProto::TError AddUnlimited(TInflightRequest& op)
    {
        std::lock_guard guard(Lock);
        if (Stopped) {
            return MakeError(E_REJECTED, "storage group is shutting down");
        }

        Requests.PushBack(&op);
        Empty.reset();

        return {};
    }

    void Remove(TInflightRequest& op)
    {
        std::lock_guard guard(Lock);

        --InflightCount;
        Unlink(op);
    }

    void RemoveUnlimited(TInflightRequest& op)
    {
        std::lock_guard guard(Lock);

        Unlink(op);
    }

    // Cancels the requests in flight and refuses every one added later.
    void Stop()
    {
        std::lock_guard guard(Lock);

        Stopped = true;
        for (auto& op: Requests) {
            op.Ctx.Cancel.set();
        }
    }

    // Until no request is in flight.
    void Wait()
    {
        Empty.wait();
    }

private:
    void Unlink(TInflightRequest& op)
    {
        op.Unlink();
        if (Requests.Empty()) {
            Empty.set();
        }
    }
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
