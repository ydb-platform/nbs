#pragma once

#include <cloud/storage/core/libs/common/verify.h>

#include <util/generic/hash_set.h>
#include <util/system/spinlock.h>

namespace NCloud::NStorage::NGrpc {

////////////////////////////////////////////////////////////////////////////////

class TRequestHandlerBase
{
private:
    std::atomic_uint64_t RefCount = 1;

public:
    virtual ~TRequestHandlerBase() = default;

    virtual void Process(bool ok) = 0;
    virtual void Cancel() = 0;

    void* AcquireCompletionTag();
    void ReleaseCompletionTag();
};

using TRequestHandlerPtr = std::unique_ptr<TRequestHandlerBase>;

// TODO(https://github.com/ydb-platform/nbs/issues/7264)
template <typename TRequestHandler>
TStringBuf RequestHandlerEntityId(const TRequestHandler& handler)
{
    if constexpr (requires { handler.EntityId; }) {
        return handler.EntityId;
    } else {
        return {};
    }
}

// EntityType is a reference (not a by-value TStringBuf) because a by-value
// class non-type template parameter must be a structural type, and TStringBuf
// is not one - it inherits std::string_view, whose members are private. A
// reference to a constexpr value (e.g. TWellKnownEntityTypes::DISK) has no such
// requirement.
template <typename TRequestHandler, const TStringBuf& EntityType>
class TRequestsInFlight final
{
protected:
    THashSet<TRequestHandler*> Requests;
    TAdaptiveLock RequestsLock;
    bool ShouldStop = false;

public:

    size_t GetCount() const
    {
        with_lock(RequestsLock) {
            return Requests.size();
        }
    }

    bool Register(TRequestHandler* handler)
    {
        with_lock(RequestsLock) {
            if (ShouldStop) {
                return false;
            }

            auto res = Requests.emplace(handler);
            STORAGE_VERIFY(
                res.second,
                EntityType,
                RequestHandlerEntityId(*handler));
        }

        return true;
    }

    void Unregister(TRequestHandler* handler)
    {
        with_lock(RequestsLock) {
            auto it = Requests.find(handler);
            STORAGE_VERIFY(
                it != Requests.end(),
                EntityType,
                RequestHandlerEntityId(*handler));
            Requests.erase(it);
        }
    }

    void Shutdown()
    {
        with_lock(RequestsLock) {
            ShouldStop = true;

            for (auto* handler : Requests) {
                handler->Cancel();
            }
        }
        TSpinWait sw;
        for(;;) {
            if (GetCount() == 0) {
                break;
            }
            sw.Sleep();
        }
    }

    template <std::invocable<TRequestHandler*> TUnaryFunction>
    void ForEach(TUnaryFunction f)
    {
        with_lock(RequestsLock) {
            for (auto* handler : Requests) {
                f(handler);
            }
        }
    }
};

}   // namespace NCloud::NStorage::NGrpc
