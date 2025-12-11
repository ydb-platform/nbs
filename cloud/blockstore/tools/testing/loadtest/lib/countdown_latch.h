#pragma once

#include "public.h"

#include <util/system/event.h>

#include <atomic>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TCountDownLatch
{
private:
    TManualEvent Event;
    std::atomic<int> Counter;

public:
    TCountDownLatch(std::atomic<int> counter)
    {
        Counter = counter.load(std::memory_order_acquire);
    }

public:
    class TGuard: TNonCopyable
    {
    private:
        TCountDownLatch& Parent;

    public:
        TGuard(TCountDownLatch& parent)
            : Parent(parent)
        {}

        ~TGuard()
        {
            Parent.CountDown();
        }
    };

    auto Guard()
    {
        return TGuard(*this);
    }

    void CountDown()
    {
        auto result = Counter.fetch_sub(1);
        Y_DEBUG_ABORT_UNLESS(result >= 0);
        if (result == 0) {
            Event.Signal();
        }
    }

    bool Done() const
    {
        return Counter.load(std::memory_order_acquire) == 0;
    }

    void Wait()
    {
        Event.WaitI();
    }
};

}   // namespace NCloud::NBlockStore::NLoadTest
