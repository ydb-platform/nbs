#pragma once

#include <cloud/fastshard/common/context.h>

#include <silk/fibers/event.h>
#include <silk/fibers/mutex.h>

#include <util/generic/intrlist.h>
#include <util/system/types.h>

#include <mutex>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

class TInflightTracker
{
public:
    struct TInflight: public TIntrusiveListItem<TInflight>
    {
        TInflightTracker& Tracker;
        NCloud::NFastShard::TFastShardContext& Context;

        TInflight(
                TInflightTracker& tracker,
                NCloud::NFastShard::TFastShardContext& context)
            : Tracker(tracker)
            , Context(context)
        {
            Tracker.Add(this);
        }

        ~TInflight()
        {
            Tracker.Remove(this);
        }
    };

private:
    silk::FiberMutex Lock;
    TIntrusiveList<TInflight> Requests;
    silk::FiberEvent Empty;
    bool Stopped = false;

public:
    TInflightTracker()
    {
        Empty.set();
    }

    TInflight Add(NCloud::NFastShard::TFastShardContext& ctx)
    {
        return {*this, ctx};
    }

    // Cancels the requests in flight and every one added later.
    void Stop()
    {
        std::lock_guard guard(Lock);
        Stopped = true;
        for (auto& request: Requests) {
            request.Context.Cancel.set();
        }
    }

    // Until no request is in flight.
    void Wait()
    {
        Empty.wait();
    }

private:
    void Add(TInflight* request)
    {
        std::lock_guard guard(Lock);
        if (Requests.Empty()) {
            Empty.reset();
        }

        Requests.PushBack(request);
        if (Stopped) {
            request->Context.Cancel.set();
        }
    }

    void Remove(TInflight* request)
    {
        std::lock_guard guard(Lock);
        request->Unlink();
        if (Requests.Empty()) {
            Empty.set();
        }
    }
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
