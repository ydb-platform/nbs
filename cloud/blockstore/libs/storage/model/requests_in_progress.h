#pragma once

#include "request_bounds_tracker.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

class TEmptyType
{
};

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

class IRequestsInProgress
{
public:
    virtual ~IRequestsInProgress() = default;

    // Returns true if there are any requests in progress
    [[nodiscard]] virtual bool WriteRequestInProgress() const = 0;

    // Mark current write/zero request as dirty.
    virtual void WaitForInFlightWrites() = 0;

    // Returns true if there are any dirty in-flight writes.
    [[nodiscard]] virtual bool IsWaitingForInFlightWrites() const = 0;
};

class IWriteRequestsTracker
{
public:
    virtual ~IWriteRequestsTracker() = default;

    // Returns true if range overlaps with any write/zero request in progress.
    [[nodiscard]] virtual bool Overlaps(TBlockRange64 range) const = 0;
};

///////////////////////////////////////////////////////////////////////////////

template <typename TKey, typename TRequest>
class TRequestsInProgressImpl: public IRequestsInProgress
{
public:
    using TRequestInfo = TRequest;

private:
    using TRequests = THashMap<TKey, TRequestInfo>;
    TRequests RequestsInProgress;
    size_t WriteRequestCount = 0;
    TKey RequestIdentityKeyCounter = {};

    // Dirty write/zero requests.
    THashSet<TKey> WaitingForWriteRequests;

public:
    ~TRequestsInProgressImpl() override = default;

    TKey GenerateRequestId()
    {
        return RequestIdentityKeyCounter++;
    }

    void SetRequestIdentityKey(TKey value)
    {
        RequestIdentityKeyCounter = value;
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

    // IRequestsInProgress  implementation

    [[nodiscard]] bool WriteRequestInProgress() const override
    {
        return WriteRequestCount != 0;
    }

    void WaitForInFlightWrites() override
    {
        for (const auto& [key, value]: RequestsInProgress) {
            if (value.IsWrite) {
                WaitingForWriteRequests.insert(key);
            }
        }
    }

    [[nodiscard]] bool IsWaitingForInFlightWrites() const override
    {
        return !WaitingForWriteRequests.empty();
    }

protected:
    void AddRequest(const TKey& key, TRequestInfo request)
    {
        Y_DEBUG_ABORT_UNLESS(!RequestsInProgress.contains(key));

        WriteRequestCount += !!request.IsWrite;
        RequestsInProgress.emplace(key, std::move(request));
    }

    TKey AddRequest(TRequestInfo request)
    {
        auto key = GenerateRequestId();
        AddRequest(key, std::move(request));
        return key;
    }

    std::optional<TRequestInfo> ExtractRequest(const TKey& key)
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
        TRequestInfo res = std::move(it->second);
        RequestsInProgress.erase(it);
        return res;
    }
};

///////////////////////////////////////////////////////////////////////////////

template <typename TValue>
struct TRequestInProgress
{
    TValue Value;
    bool IsWrite = false;
};

template <EAllowedRequests TKind, typename TKey, typename TValue = TEmptyType>
class TRequestsInProgress
    : public TRequestsInProgressImpl<TKey, TRequestInProgress<TValue>>
{
public:
    using TRequestInfo = TRequestInProgress<TValue>;

private:
    using TImpl = TRequestsInProgressImpl<TKey, TRequestInfo>;

public:
    ~TRequestsInProgress() = default;

    void AddReadRequest(const TKey& key, TValue value = {})
        requires(IsReadAllowed<TKind>)
    {
        TImpl::AddRequest(
            key,
            TRequestInfo{.Value = std::move(value), .IsWrite = false});
    }

    TKey AddReadRequest(TValue value)
        requires(IsReadAllowed<TKind>)
    {
        return TImpl::AddRequest(
            TRequestInfo{.Value = std::move(value), .IsWrite = false});
    }

    void AddWriteRequest(const TKey& key, TValue value = {})
        requires(IsWriteAllowed<TKind>)
    {
        TImpl::AddRequest(
            key,
            TRequestInfo{.Value = std::move(value), .IsWrite = true});
    }

    TKey AddWriteRequest(TValue value)
        requires(IsWriteAllowed<TKind>)
    {
        return TImpl::AddRequest(
            TRequestInfo{.Value = std::move(value), .IsWrite = true});
    }

    bool RemoveRequest(const TKey& key)
    {
        return TImpl::ExtractRequest(key).has_value();
    }

    std::optional<TRequestInfo> ExtractRequest(const TKey& key)
    {
        return TImpl::ExtractRequest(key);
    }
};

///////////////////////////////////////////////////////////////////////////////

template <typename TValue>
struct TRequestInProgressWithBlockRange
{
    TValue Value;
    bool IsWrite = false;
    TBlockRange64 BlockRange;
};

template <EAllowedRequests TKind, typename TKey, typename TValue = TEmptyType>
class TRequestsInProgressWithBlockRangeTracking
    : public TRequestsInProgressImpl<
          TKey,
          TRequestInProgressWithBlockRange<TValue>>
    , public IWriteRequestsTracker
{
public:
    using TRequestInfo = TRequestInProgressWithBlockRange<TValue>;

private:
    using TImpl = TRequestsInProgressImpl<TKey, TRequestInfo>;

    TRequestBoundsTracker WriteRequestBoundsTracker;

public:
    explicit TRequestsInProgressWithBlockRangeTracking(size_t blockSize)
        : WriteRequestBoundsTracker(blockSize)
    {}

    void AddReadRequest(const TKey& key, TBlockRange64 range, TValue value)
        requires(IsReadAllowed<TKind>)
    {
        TImpl::AddRequest(
            key,
            {.Value = std::move(value), .IsWrite = false, .BlockRange = range});
    }

    TKey AddReadRequest(TBlockRange64 range, TValue value)
        requires(IsReadAllowed<TKind>)
    {
        return TImpl::AddRequest(
            {.Value = std::move(value), .IsWrite = false, .BlockRange = range});
    }

    void AddWriteRequest(const TKey& key, TBlockRange64 range, TValue value)
        requires(IsWriteAllowed<TKind>)
    {
        WriteRequestBoundsTracker.AddRequest(range);

        TImpl::AddRequest(
            key,
            {.Value = std::move(value), .IsWrite = true, .BlockRange = range});
    }

    TKey AddWriteRequest(TBlockRange64 range, TValue value)
        requires(IsWriteAllowed<TKind>)
    {
        TKey key = this->GenerateRequestId();
        AddWriteRequest(key, range, std::move(value));
        return key;
    }

    bool RemoveRequest(const TKey& key)
    {
        return ExtractRequest(key).has_value();
    }

    std::optional<TRequestInfo> ExtractRequest(const TKey& key)
    {
        auto request = TImpl::ExtractRequest(key);
        if (request && request->IsWrite) {
            WriteRequestBoundsTracker.RemoveRequest(request->BlockRange);
        }

        return request;
    }

    // IRequestsInProgressWithBlockRangeTracking implementation.

    [[nodiscard]] bool Overlaps(TBlockRange64 range) const override
    {
        return WriteRequestBoundsTracker.OverlapsWithRequest(range);
    }
};

}   // namespace NCloud::NBlockStore::NStorage
