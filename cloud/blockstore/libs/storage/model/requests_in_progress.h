#pragma once

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
    [[nodiscard]] virtual bool HasWriteRequestsInRange(TBlockRange64 r) const = 0;
};

///////////////////////////////////////////////////////////////////////////////

class TRequestBoundsChecker
{
    struct TRangeInfo
    {
        ui64 ReqsCount = 0;
    };

    THashMap<ui64, TRangeInfo> RangesWithRequests;
    const ui64 BlocksCountPerRange;

public:
    explicit TRequestBoundsChecker(ui64 blockSize)
        : BlocksCountPerRange(blockSize ? 4_MB / blockSize : 0)
    {}

    void AddRequest(TBlockRange64 r)
    {
        CheckIsValidToUse();

        auto startRange = r.Start / BlocksCountPerRange;
        auto endRange = r.End / BlocksCountPerRange;
        for (size_t i = startRange; i <= endRange; ++i) {
            auto& rangeInfo = RangesWithRequests[i];
            rangeInfo.ReqsCount += 1;
        }
    }

    void RmRequest(TBlockRange64 r)
    {
        CheckIsValidToUse();

        auto startRange = r.Start / BlocksCountPerRange;
        auto endRange = r.End / BlocksCountPerRange;
        for (size_t i = startRange; i <= endRange; ++i) {
            auto it = RangesWithRequests.find(i);

            Y_DEBUG_ABORT_UNLESS(it != RangesWithRequests.end());
            if (it == RangesWithRequests.end()) {
                continue;
            }

            auto& rangeInfo = it->second;
            rangeInfo.ReqsCount -= 1;
            if (rangeInfo.ReqsCount == 0) {
                RangesWithRequests.erase(it);
            }
        }
    }

    bool OverlapsSomeRange(TBlockRange64 r) const
    {
        CheckIsValidToUse();

        auto startRange = r.Start / BlocksCountPerRange;
        auto endRange = r.End / BlocksCountPerRange;
        for (size_t i = startRange; i <= endRange; ++i) {
            auto it = RangesWithRequests.find(i);
            if (it != RangesWithRequests.end()) {
                return true;
            }
        }

        return false;
    }
    private:

    void CheckIsValidToUse() const {
        Y_ABORT_UNLESS(BlocksCountPerRange != 0);
    }
};

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

    TRequestBoundsChecker BlockRangesForWriteRequestsInProgres;

    TRequests RequestsInProgress;
    size_t WriteRequestCount = 0;
    TKey RequestIdentityKeyCounter = {};

public:
    explicit TRequestsInProgress(
            EAllowedRequests allowedRequests,
            std::optional<ui64> blocksize = std::nullopt)
        : AllowedRequests(allowedRequests)
        , BlockRangesForWriteRequestsInProgres(blocksize.value_or(0))
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

        BlockRangesForWriteRequestsInProgres.AddRequest(range);
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
            BlockRangesForWriteRequestsInProgres.RmRequest(
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

    [[nodiscard]] bool HasWriteRequestsInRange(TBlockRange64 r) const override
    {
        return BlockRangesForWriteRequestsInProgres.OverlapsSomeRange(r);
    }
};

}  // namespace NCloud::NBlockStore::NStorage
