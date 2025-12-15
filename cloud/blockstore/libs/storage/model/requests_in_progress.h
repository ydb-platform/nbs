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
        return WriteRequests.FindFirstOverlapping(range) != nullptr;
    }

    void WaitForInFlightWrites() override
    {
        DirtyWriteRequests = WriteRequests.GetAllKeys();
    }

    // IRequestsInProgressWithBlockRangeTracking implementation.

    [[nodiscard]] bool Overlaps(TBlockRange64 range) const override
    {
        return !DirtyWriteRequests.empty();
    }

    void EnumerateReadOverlapping(TBlockRange64 range, TEnumerateFunc f) const
        requires(IsReadAllowed<TKind>)
    {
        ReadRequests.EnumerateOverlapping(
            range,
            [&](const TRequests::TItem& item)
            {
                f(item.Key,
                  false,   // Read
                  item.Range,
                  item.Value);
                return TRequests::EEnumerateContinuation::Continue;
            });
    }

    void EnumerateRequests(TEnumerateFunc f) const
    {
        ReadRequests.Enumerate(
            [&](const TRequests::TItem& item)
            {
                f(item.Key,
                  false,   // Read
                  item.Range,
                  item.Value);
                return TRequests::EEnumerateContinuation::Continue;
            });
        WriteRequests.Enumerate(
            [&](const TRequests::TItem& item)
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
