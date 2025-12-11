#include "limiter.h"

namespace NCloud::NBlockStore::NBD {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TLimiter final: public ILimiter
{
private:
    TLog Log;
    TContSimpleEvent Event;

    const size_t MaxInFlightBytes;
    size_t InFlightBytes = 0;

public:
    TLimiter(const TLog& log, TContExecutor* e, size_t maxInFlightBytes)
        : Log(log)
        , Event(e)
        , MaxInFlightBytes(maxInFlightBytes)
    {}

    bool Acquire(size_t requestBytes) override;
    void Release(size_t requestBytes) override;
};

////////////////////////////////////////////////////////////////////////////////

bool TLimiter::Acquire(size_t requestBytes)
{
    if (InFlightBytes > MaxInFlightBytes) {
        STORAGE_DEBUG("request processing blocked by in-flight limit");
        do {
            int result = Event.WaitI();
            if (result == ECANCELED) {
                return false;
            }
        } while (InFlightBytes > MaxInFlightBytes);
    }

    InFlightBytes += requestBytes;
    return true;
}

void TLimiter::Release(size_t requestBytes)
{
    size_t prev = InFlightBytes;
    Y_ABORT_UNLESS(prev >= requestBytes);

    size_t next = prev - requestBytes;
    InFlightBytes = next;

    if (prev > MaxInFlightBytes && next <= MaxInFlightBytes) {
        STORAGE_DEBUG("request processing unblocked");
        Event.BroadCast();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TLimiterStub final: public ILimiter
{
public:
    bool Acquire(size_t requestBytes) override
    {
        Y_UNUSED(requestBytes);
        return true;
    }

    void Release(size_t requestBytes) override
    {
        Y_UNUSED(requestBytes);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ILimiterPtr
CreateLimiter(const TLog& log, TContExecutor* e, size_t maxInFlightBytes)
{
    return std::make_shared<TLimiter>(log, e, maxInFlightBytes);
}

ILimiterPtr CreateLimiterStub()
{
    return std::make_shared<TLimiterStub>();
}

}   // namespace NCloud::NBlockStore::NBD
