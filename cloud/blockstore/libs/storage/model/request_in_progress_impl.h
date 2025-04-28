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

template <EAllowedRequests TKind>
constexpr bool IsWriteAllowed = TKind == EAllowedRequests::ReadWrite ||
                                TKind == EAllowedRequests::WriteOnly;

template <EAllowedRequests TKind>
constexpr bool IsReadAllowed =
    TKind == EAllowedRequests::ReadOnly || TKind == EAllowedRequests::ReadWrite;

///////////////////////////////////////////////////////////////////////////////

template <typename TKey, typename TRequest>
    requires requires(TRequest t) {
        {
            t.IsWrite
        } -> std::convertible_to<bool>;
    }
class TRequestsInProgressImpl
{
    using TRequests = THashMap<TKey, TRequest>;

private:
    TRequests RequestsInProgress;
    size_t WriteRequestCount = 0;
    TKey RequestIdentityKeyCounter = {};

    THashSet<TKey> WaitingForWriteRequests;

public:
    ~TRequestsInProgressImpl() = default;

    TKey GenerateRequestId()
    {
        return RequestIdentityKeyCounter++;
    }

    void SetRequestIdentityKey(TKey value)
    {
        RequestIdentityKeyCounter = value;
    }

    void AddRequest(const TKey& key, TRequest request)
    {
        Y_DEBUG_ABORT_UNLESS(!RequestsInProgress.contains(key));

        RequestsInProgress.emplace(key, std::move(request));
        WriteRequestCount += !!request.IsWrite;
    }

    TKey AddRequest(TRequest request)
    {
        auto key = GenerateRequestId();
        AddRequest(key, std::move(request));
        return key;
    }

    TRequest GetRequest(const TKey& key) const
    {
        if (auto* requestInfo = RequestsInProgress.FindPtr(key)) {
            return *requestInfo;
        }
        Y_DEBUG_ABORT_UNLESS(0);

        return {};
    }

    std::optional<TRequest> ExtractRequest(const TKey& key)
    {
        auto it = RequestsInProgress.find(key);

        if (it == RequestsInProgress.end()) {
            Y_DEBUG_ABORT_UNLESS(0);
            return std::nullopt;
        }

        if (it->second.IsWrite) {
            WaitingForWriteRequests.erase(key);
            --WriteRequestCount;
        }
        TRequest res = std::move(it->second);
        RequestsInProgress.erase(it);
        return res;
    }

    const TRequests& AllRequests() const
    {
        return RequestsInProgress;
    }

    [[nodiscard]] size_t GetRequestCount() const
    {
        return RequestsInProgress.size();
    }

    [[nodiscard]] bool Empty() const
    {
        return GetRequestCount() == 0;
    }

    [[nodiscard]] bool WriteRequestInProgress() const
    {
        return WriteRequestCount != 0;
    }

    void WaitForInFlightWrites()
    {
        for (const auto& [key, value]: RequestsInProgress) {
            if (value.IsWrite) {
                WaitingForWriteRequests.insert(key);
            }
        }
    }

    [[nodiscard]] bool IsWaitingForInFlightWrites() const
    {
        return !WaitingForWriteRequests.empty();
    }
};

}   // namespace NCloud::NBlockStore::NStorage
