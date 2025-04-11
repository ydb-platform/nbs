#pragma once

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

enum class EAllowedRequests
{
    ReadOnly,
    WriteOnly,
    ReadWrite,
};

///////////////////////////////////////////////////////////////////////////////

class TEmptyType
{
};

///////////////////////////////////////////////////////////////////////////////

class IRequestsInProgress
{
public:
    virtual ~IRequestsInProgress() = default;

    [[nodiscard]] virtual bool WriteRequestInProgress() const = 0;

    virtual void WaitForInFlightWrites() = 0;
    [[nodiscard]] virtual bool IsWaitingForInFlightWrites() const = 0;
};

///////////////////////////////////////////////////////////////////////////////

template <typename TKey, typename TValue = TEmptyType>
class TRequestsInProgress: public IRequestsInProgress
{
public:
    struct TRequest
    {
        TValue Value;
        bool Write = false;
    };
    using TRequests = THashMap<TKey, TRequest>;

private:
    EAllowedRequests AllowedRequests;
    TRequests RequestsInProgress;
    size_t WriteRequestCount = 0;
    TKey RequestIdentityKeyCounter = {};

    THashSet<TKey> WaitingForWriteRequests;

public:
    explicit TRequestsInProgress(EAllowedRequests allowedRequests)
        : AllowedRequests(allowedRequests)
    {}

    ~TRequestsInProgress() = default;

    TKey GenerateRequestId()
    {
        return RequestIdentityKeyCounter++;
    }

    void SetRequestIdentityKey(TKey value)
    {
        RequestIdentityKeyCounter = value;
    }

    void AddReadRequest(const TKey& key, TValue value = {})
    {
        Y_DEBUG_ABORT_UNLESS(!RequestsInProgress.contains(key));
        Y_DEBUG_ABORT_UNLESS(
            AllowedRequests == EAllowedRequests::ReadOnly ||
            AllowedRequests == EAllowedRequests::ReadWrite);
        RequestsInProgress.emplace(key, TRequest{std::move(value), false});
    }

    TKey AddReadRequest(TValue value)
    {
        TKey key = RequestIdentityKeyCounter++;
        AddReadRequest(key, std::move(value));
        return key;
    }

    void AddWriteRequest(const TKey& key, TValue value = {})
    {
        Y_DEBUG_ABORT_UNLESS(!RequestsInProgress.contains(key));
        Y_DEBUG_ABORT_UNLESS(
            AllowedRequests == EAllowedRequests::WriteOnly ||
            AllowedRequests == EAllowedRequests::ReadWrite);
        RequestsInProgress.emplace(key, TRequest{std::move(value), true});
        ++WriteRequestCount;
    }

    TKey AddWriteRequest(TValue value)
    {
        TKey key = RequestIdentityKeyCounter++;
        AddWriteRequest(key, std::move(value));
        return key;
    }

    TValue GetRequest(const TKey& key) const
    {
        if (auto* requestInfo = RequestsInProgress.FindPtr(key)) {
            return requestInfo->Value;
        } else {
            Y_DEBUG_ABORT_UNLESS(0);
        }
        return {};
    }

    bool RemoveRequest(const TKey& key)
    {
        return ExtractRequest(key).has_value();
    }

    std::optional<TValue> ExtractRequest(const TKey& key)
    {
        auto it = RequestsInProgress.find(key);

        if (it == RequestsInProgress.end()) {
            Y_DEBUG_ABORT_UNLESS(0);
            return std::nullopt;
        }

        if (it->second.Write) {
            WaitingForWriteRequests.erase(key);
            --WriteRequestCount;
        }
        TValue res = std::move(it->second.Value);
        RequestsInProgress.erase(it);
        return res;
    }

    const TRequests& AllRequests() const
    {
        return RequestsInProgress;
    }

    size_t GetRequestCount() const
    {
        return RequestsInProgress.size();
    }

    bool Empty() const
    {
        return GetRequestCount() == 0;
    }

    bool WriteRequestInProgress() const override
    {
        return WriteRequestCount != 0;
    }

    void WaitForInFlightWrites() override
    {
        for (const auto& [key, value]: RequestsInProgress) {
            if (value.Write) {
                WaitingForWriteRequests.insert(key);
            }
        }
    }

    [[nodiscard]] bool IsWaitingForInFlightWrites() const override
    {
        return !WaitingForWriteRequests.empty();
    }
};

}  // namespace NCloud::NBlockStore::NStorage
