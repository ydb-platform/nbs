#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/common/block_range_map.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>

#include <util/generic/hash.h>
#include <util/string/cast.h>

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

    // Returns true if range overlaps with any write/zero request in progress.
    [[nodiscard]] virtual bool OverlapsWithWrites(
        TBlockRange64 range) const = 0;

    // Mark current write/zero request as dirty.
    virtual void WaitForInFlightWrites() = 0;

    // Returns true if there are any dirty in-flight writes.
    [[nodiscard]] virtual bool IsWaitingForInFlightWrites() const = 0;
};

///////////////////////////////////////////////////////////////////////////////

template <
    EAllowedRequests TKind,
    typename TKey,
    typename TRequestInfo = TEmptyType>
class TRequestsInProgress: public IRequestsInProgress
{
public:
    using TRequests = TBlockRangeMap<TKey, TRequestInfo>;
    using TItem = TRequests::TItem;
    using TEnumerateFunc =
        std::function<void(TKey, bool, TBlockRange64, const TRequestInfo&)>;

private:
    TKey RequestIdentityKeyCounter = {};
    TRequests ReadRequests;
    TRequests WriteRequests;
    THashSet<TKey> DirtyWriteRequests;

public:
    ~TRequestsInProgress() override = default;

    TKey GenerateRequestId()
    {
        return RequestIdentityKeyCounter++;
    }

    void SetRequestIdentityKey(TKey value)
    {
        RequestIdentityKeyCounter = value;
    }

    [[nodiscard]] size_t GetRequestCount() const
    {
        return ReadRequests.Size() + WriteRequests.Size();
    }

    [[nodiscard]] bool Empty() const
    {
        return ReadRequests.Empty() && WriteRequests.Empty();
    }

    // IRequestsInProgress  implementation

    [[nodiscard]] bool WriteRequestInProgress() const override
    {
        return !WriteRequests.Empty();
    }

    [[nodiscard]] bool OverlapsWithWrites(TBlockRange64 range) const override
    {
        return WriteRequests.HasOverlaps(range);
    }

    void WaitForInFlightWrites() override
    {
        DirtyWriteRequests = WriteRequests.GetAllKeys();
    }

    [[nodiscard]] bool IsWaitingForInFlightWrites() const override
    {
        return !DirtyWriteRequests.empty();
    }

    void EnumerateReadOverlapping(TBlockRange64 range, TEnumerateFunc f)
        requires(IsReadAllowed<TKind>)
    {
        ReadRequests.EnumerateOverlapping(
            range,
            [&](const TRequests::TFindItem& item)
            {
                f(item.Key,
                  false,   // Read
                  item.Range,
                  item.Value);
                return TRequests::EEnumerateContinuation::Continue;
            });
    }

    void EnumerateRequests(TEnumerateFunc f)
    {
        ReadRequests.Enumerate(
            [&](const TRequests::TFindItem& item)
            {
                f(item.Key,
                  false,   // Read
                  item.Range,
                  item.Value);
                return TRequests::EEnumerateContinuation::Continue;
            });
        WriteRequests.Enumerate(
            [&](const TRequests::TFindItem& item)
            {
                f(item.Key,
                  true,   // Write
                  item.Range,
                  item.Value);
                return TRequests::EEnumerateContinuation::Continue;
            });
    }

    void AddWriteRequest(
        const TKey& key,
        TBlockRange64 range,
        TRequestInfo request = {})
        requires(IsWriteAllowed<TKind>)
    {
        const bool inserted =
            WriteRequests.AddRange(key, range, std::move(request));
        if (!inserted) {
            ReportInflightRequestInvariantViolation(
                "Failed to register write request",
                {{"key", ToString(key)}, {"range", range}});
        }
    }

    TKey AddWriteRequest(TBlockRange64 range, TRequestInfo request = {})
        requires(IsWriteAllowed<TKind>)
    {
        const auto key = GenerateRequestId();
        AddWriteRequest(key, range, std::move(request));
        return key;
    }

    void AddReadRequest(
        const TKey& key,
        TBlockRange64 range,
        TRequestInfo request = {})
        requires(IsReadAllowed<TKind>)
    {
        const bool inserted =
            ReadRequests.AddRange(key, range, std::move(request));
        if (!inserted) {
            ReportInflightRequestInvariantViolation(
                "Failed to register read request",
                {{"key", ToString(key)}, {"range", range}});
        }
    }

    TKey AddReadRequest(TBlockRange64 range, TRequestInfo request = {})
        requires(IsReadAllowed<TKind>)
    {
        const auto key = GenerateRequestId();
        AddReadRequest(key, range, std::move(request));
        return key;
    }

    std::optional<TItem> ExtractReadRequest(const TKey& key)
        requires(IsReadAllowed<TKind>)
    {
        return ReadRequests.ExtractRange(key);
    }

    std::optional<TItem> ExtractWriteRequest(const TKey& key)
        requires(IsWriteAllowed<TKind>)
    {
        DirtyWriteRequests.erase(key);
        return WriteRequests.ExtractRange(key);
    }

    bool RemoveReadRequest(const TKey& key)
        requires(IsReadAllowed<TKind>)
    {
        return ExtractReadRequest(key).has_value();
    }

    bool RemoveWriteRequest(const TKey& key)
        requires(IsWriteAllowed<TKind>)
    {
        return ExtractWriteRequest(key).has_value();
    }

    std::optional<TItem> ExtractRequest(const TKey& key)
    {
        if constexpr (IsReadAllowed<TKind>) {
            if (auto result = ExtractReadRequest(key)) {
                return result;
            }
        }
        if constexpr (IsWriteAllowed<TKind>) {
            if (auto result = ExtractWriteRequest(key)) {
                return result;
            }
        }
        return std::nullopt;
    }
};

///////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
