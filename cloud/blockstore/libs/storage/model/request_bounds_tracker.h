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

    THashMap<ui64, TRangeInfo> RangesWithRequests;
    const ui64 BlockCountPerRange;

public:
    explicit TRequestBoundsTracker(ui64 blockSize);

    void AddRequest(TBlockRange64 r);

    void RemoveRequest(TBlockRange64 r);

    [[nodiscard]] bool OverlapsWithRequest(TBlockRange64 r) const;
};

template <typename TValue>
struct TIsWriteBlockRangeValueWrapper
{
    TValue Value;
    bool IsWrite = false;
    TBlockRange64 BlockRange;
};

template <EAllowedRequests TKind, typename TKey, typename TValue = TEmptyType>
class TRequestsInProgressWithBlockRangeTracking
    : public IRequestsInProgress
    , TRequestsInProgressImpl<TKey, TIsWriteBlockRangeValueWrapper<TValue>>
{
    using TImpl =
        TRequestsInProgressImpl<TKey, TIsWriteBlockRangeValueWrapper<TValue>>;
    using TRangeRequest = TIsWriteBlockRangeValueWrapper<TValue>;

private:
    TRequestBoundsTracker WriteRequestBoundsTracker;

public:
    explicit TRequestsInProgressWithBlockRangeTracking(size_t blockSize)
        : WriteRequestBoundsTracker(blockSize)
    {}

    using TImpl::AllRequests;
    using TImpl::Empty;
    using TImpl::GenerateRequestId;
    using TImpl::GetRequestCount;
    using TImpl::SetRequestIdentityKey;

    void AddReadRequest(const TKey& key, TBlockRange64 range, TValue value = {})
        requires(IsReadAllowed<TKind>)
    {
        TImpl::AddRequest(key, TRangeRequest{std::move(value), false, range});
    }

    TKey AddReadRequest(TBlockRange64 range, TValue value = {})
        requires(IsReadAllowed<TKind>)
    {
        return TImpl::AddRequest(TRangeRequest{std::move(value), false, range});
    }

    void
    AddWriteRequest(const TKey& key, TBlockRange64 range, TValue value = {})
        requires(IsWriteAllowed<TKind>)
    {
        WriteRequestBoundsTracker.AddRequest(range);

        TImpl::AddRequest(
            std::move(key),
            TRangeRequest{std::move(value), true, range});
    }

    TKey AddWriteRequest(TBlockRange64 range, TValue value = {})
        requires(IsWriteAllowed<TKind>)
    {
        TKey key = GenerateRequestId();
        AddWriteRequest(key, range, std::move(value));
        return key;
    }

    TValue GetRequest(const TKey& key) const
    {
        return TImpl::GetRequest(key).Value;
    }

    bool RemoveRequest(const TKey& key)
    {
        return ExtractRequest(key).has_value();
    }

    std::optional<std::pair<TBlockRange64, TValue>> ExtractRequest(
        const TKey& key)
    {
        auto maybeValue = TImpl::ExtractRequest(key);
        if (!maybeValue) {
            return std::nullopt;
        }

        if (maybeValue->IsWrite) {
            WriteRequestBoundsTracker.RemoveRequest(maybeValue->BlockRange);
        }

        return std::make_pair(
            maybeValue->BlockRange,
            std::move(maybeValue->Value));
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
