#pragma once

#include "request_bounds_tracker.h"

#include <cloud/blockstore/libs/common/block_range.h>

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
    [[nodiscard]] virtual bool WriteRequestInProgress() const = 0;
    [[nodiscard]] virtual bool HasWriteRequestInRange(TBlockRange64 r) const = 0;
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
        std::optional<TBlockRange64> RequestRange;
    };
    using TRequests = THashMap<TKey, TRequest>;

private:
    EAllowedRequests AllowedRequests;

    std::optional<TRequestBoundsTracker> WriteRequestRangesTracker;

    TRequests RequestsInProgress;
    size_t WriteRequestCount = 0;
    TKey RequestIdentityKeyCounter = {};

public:
    explicit TRequestsInProgress(
            EAllowedRequests allowedRequests,
            std::optional<ui64> blockSize = std::nullopt)
        : AllowedRequests(allowedRequests)
    {
        if (blockSize) {
            WriteRequestRangesTracker.emplace(*blockSize);
        }
    }

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
        RequestsInProgress.emplace(
            key,
            TRequest{.Value = std::move(value), .Write = false});
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
        RequestsInProgress.emplace(
            key,
            TRequest{.Value = std::move(value), .Write = true});
        ++WriteRequestCount;
    }

    void AddWriteRequestWithBlockRangeTracking(
        const TKey& key,
        TBlockRange64 range,
        TValue value = {})
    {
        Y_DEBUG_ABORT_UNLESS(!RequestsInProgress.contains(key));
        Y_DEBUG_ABORT_UNLESS(
            AllowedRequests == EAllowedRequests::WriteOnly ||
            AllowedRequests == EAllowedRequests::ReadWrite);
        RequestsInProgress.emplace(
            key,
            TRequest{
                .Value = std::move(value),
                .Write = true,
                .RequestRange = range});
        ++WriteRequestCount;

        WriteRequestRangesTracker->AddRequest(range);
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
            --WriteRequestCount;
        }
        if (it->second.RequestRange) {
            WriteRequestRangesTracker->RemoveRequest(
                *it->second.RequestRange);
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

    [[nodiscard]] bool WriteRequestInProgress() const override
    {
        return WriteRequestCount != 0;
    }

    [[nodiscard]] bool HasWriteRequestInRange(TBlockRange64 r) const override
    {
        return WriteRequestRangesTracker->OverlapsSomeRange(r);
    }
};

}  // namespace NCloud::NBlockStore::NStorage
