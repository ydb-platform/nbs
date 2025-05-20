#pragma once

#include "requests_in_progress.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

class TRequestBoundsTracker
{
    struct TRangeInfo
    {
        ui64 RequestCount = 0;
    };

private:
    THashMap<ui64, TRangeInfo> RangesWithRequests;
    const ui64 BlockCountPerRange;

public:
    explicit TRequestBoundsTracker(ui64 blockSize);

    void AddRequest(TBlockRange64 r);

    void RemoveRequest(TBlockRange64 r);

    [[nodiscard]] bool OverlapsWithRequest(TBlockRange64 r) const;
};

template <typename TValue>
struct TRequestInProgressWithBlockRange
{
    TValue Value;
    bool IsWrite = false;
    TBlockRange64 BlockRange;
};

template <EAllowedRequests TKind, typename TKey, typename TValue = TEmptyType>
class TRequestsInProgressWithBlockRangeTracking
    : public IRequestsInProgress
    , TRequestsInProgressImpl<TKey, TRequestInProgressWithBlockRange<TValue>>
{
    using TImpl =
        TRequestsInProgressImpl<TKey, TRequestInProgressWithBlockRange<TValue>>;

private:
    TRequestBoundsTracker WriteRequestBoundsTracker;

public:
    using TRequest = TRequestInProgressWithBlockRange<TValue>;

public:
    explicit TRequestsInProgressWithBlockRangeTracking(size_t blockSize)
        : WriteRequestBoundsTracker(blockSize)
    {}

    using TImpl::AllRequests;
    using TImpl::Empty;
    using TImpl::GenerateRequestId;
    using TImpl::GetRequest;
    using TImpl::GetRequestCount;
    using TImpl::SetRequestIdentityKey;

    void AddReadRequest(const TKey& key, TBlockRange64 range, TValue value = {})
        requires(IsReadAllowed<TKind>)
    {
        TImpl::AddRequest(
            key,
            {.Value = std::move(value), .IsWrite = false, .BlockRange = range});
    }

    TKey AddReadRequest(TBlockRange64 range, TValue value = {})
        requires(IsReadAllowed<TKind>)
    {
        return TImpl::AddRequest(
            {.Value = std::move(value), .IsWrite = false, .BlockRange = range});
    }

    void
    AddWriteRequest(const TKey& key, TBlockRange64 range, TValue value = {})
        requires(IsWriteAllowed<TKind>)
    {
        WriteRequestBoundsTracker.AddRequest(range);

        TImpl::AddRequest(
            key,
            {.Value = std::move(value), .IsWrite = true, .BlockRange = range});
    }

    TKey AddWriteRequest(TBlockRange64 range, TValue value = {})
        requires(IsWriteAllowed<TKind>)
    {
        TKey key = GenerateRequestId();
        AddWriteRequest(key, range, std::move(value));
        return key;
    }

    bool RemoveRequest(const TKey& key)
    {
        return ExtractRequest(key).has_value();
    }

    std::optional<TRequest> ExtractRequest(const TKey& key)
    {
        auto request = TImpl::ExtractRequest(key);
        if (request && request->IsWrite) {
            WriteRequestBoundsTracker.RemoveRequest(request->BlockRange);
        }

        return request;
    }

    const TRequestBoundsTracker& GetRequestBoundsTracker()
    {
        return WriteRequestBoundsTracker;
    }

    // IRequestsInProgress

    [[nodiscard]] bool WriteRequestInProgress() const override
    {
        return TImpl::WriteRequestInProgress();
    }

    void WaitForInFlightWrites() override
    {
        TImpl::WaitForInFlightWrites();
    }

    [[nodiscard]] bool IsWaitingForInFlightWrites() const override
    {
        return TImpl::IsWaitingForInFlightWrites();
    }
};

}   // namespace NCloud::NBlockStore::NStorage
