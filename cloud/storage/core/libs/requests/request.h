#pragma once

#include <library/cpp/threading/atomic/bool.h>

#include <util/generic/hash_set.h>
#include <util/system/spinlock.h>

namespace NCloud::NStorage::NRequests {

////////////////////////////////////////////////////////////////////////////////

class TRequestHandlerBase
{
private:
    TAtomic RefCount = 0;

public:
    virtual ~TRequestHandlerBase() = default;

    virtual void Process(bool ok) = 0;
    virtual void Cancel() = 0;

    void* AcquireCompletionTag();
    bool ReleaseCompletionTag();
};

using TRequestHandlerPtr = std::unique_ptr<TRequestHandlerBase>;

template<std::derived_from<TRequestHandlerBase> TRequestHandler>
class TRequestsInFlight
{
protected:
    THashSet<TRequestHandler*> Requests;
    TAdaptiveLock RequestsLock;

public:
    virtual ~TRequestsInFlight() = default;

    size_t GetCount() const
    {
        with_lock(RequestsLock)
        {
            return Requests.size();
        }
    }

    void Register(TRequestHandler* handler)
    {
        with_lock(RequestsLock)
        {
            auto res = Requests.emplace(handler);
            Y_VERIFY(res.second);
        }
    }

    void Unregister(TRequestHandler* handler)
    {
        with_lock(RequestsLock)
        {
            auto it = Requests.find(handler);
            Y_VERIFY(it != Requests.end());
            Requests.erase(it);
        }
    }

    void CancelAll()
    {
        with_lock(RequestsLock)
        {
            for (auto* handler : Requests) {
                handler->Cancel();
            }
        }
    }
};

}   // namespace NCloud::NStorage::NRequests
