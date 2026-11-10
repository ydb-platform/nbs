#pragma once

#include <cloud/fastshard/libs/common/context.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/event.h>
#include <silk/fibers/mutex.h>

#include <util/generic/intrlist.h>
#include <util/system/types.h>

#include <atomic>

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
    NProto::TError Add(TInflightRequest& op);

    void Remove(TInflightRequest& op);

    ui32 GetInflightCount() const
    {
        return InflightCount.load(std::memory_order_relaxed);
    }

    // Cancels the requests in flight, refuses new ones, waits for the rest.
    void Stop();
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
