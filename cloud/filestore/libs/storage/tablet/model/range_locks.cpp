#include "range_locks.h"

#include <util/digest/multi.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>
#include <util/string/cast.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

std::tuple<ui64, ui64> SafeRange(ui64 offset, ui64 length)
{
    static constexpr ui64 MaxOffset = Max<ui64>();
    if (!length) {
        // trying to lock file till the end of the file
        length = MaxOffset;
    }

    if (offset < MaxOffset - length) {
        return {offset, offset + length};
    }
    return {offset, MaxOffset};
}

////////////////////////////////////////////////////////////////////////////////

struct TLock: TSimpleRefCount<TLock>
{
    ui64 LockId;
    ELockMode Mode;
    pid_t Pid;

    TLock(ui64 lockId, ELockMode mode, pid_t pid)
        : LockId(lockId)
        , Mode(mode)
        , Pid(pid)
    {}

    bool IsCompatible(ELockMode mode) const
    {
        // only shared locks are compatible
        return Mode == ELockMode::Shared && mode == ELockMode::Shared;
    }
};

using TLockPtr = TIntrusivePtr<TLock>;

////////////////////////////////////////////////////////////////////////////////

struct TRange
{
    /*const*/ ui64 Min;
    const ui64 Max;
    const TLockPtr Lock;

    TRange(ui64 min, ui64 max, TLockPtr lock)
        : Min(min)
        , Max(max)
        , Lock(std::move(lock))
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TLockedRanges
{
private:
    TMap<ui64, TRange> RangesByMax;

public:
    using const_iterator = TMap<ui64, TRange>::const_iterator;

    bool Insert(ui64 min, ui64 max, TLockPtr lock)
    {
        return RangesByMax.emplace(max, TRange(min, max, std::move(lock)))
            .second;
    }

    TVector<ui64> Erase(ui64 min, ui64 max)
    {
        auto it = RangesByMax.upper_bound(min);
        if (it == RangesByMax.end()) {
            return {};
        }
        if (it->second.Min < min) {
            auto& range = it->second;
            // split range
            bool inserted = Insert(range.Min, min, range.Lock);
            Y_ABORT_UNLESS(inserted);

            // trim left range
            range.Min = min;
        }

        // delete ranges
        TVector<ui64> removedLocks;
        while (it != RangesByMax.end() && max >= it->first) {
            if (it->second.Lock.RefCount() == 1) {
                removedLocks.push_back(it->second.Lock->LockId);
            }
            it = RangesByMax.erase(it);
        }

        // trim right range
        if (it != RangesByMax.end() && it->second.Min < max) {
            it->second.Min = max;
        }
        return removedLocks;
    }

    const_iterator
    FindConflictingRange(ui64 min, ui64 max, ELockMode mode) const
    {
        auto it = RangesByMax.upper_bound(min);
        while (it != RangesByMax.end() && it->second.Min < max) {
            if (!it->second.Lock->IsCompatible(mode)) {
                return it;
            }
            ++it;
        }
        return RangesByMax.end();
    }

    const_iterator End() const
    {
        return RangesByMax.end();
    }

    bool Empty() const
    {
        return RangesByMax.empty();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TOwner
{
    const TString SessionId;
    const ui64 OwnerId;

    TOwner(TString sessionId, ui64 ownerId)
        : SessionId(std::move(sessionId))
        , OwnerId(ownerId)
    {}

    bool Equal(const TOwner& other) const
    {
        return SessionId == other.SessionId && OwnerId == other.OwnerId;
    }

    size_t GetHash() const
    {
        return MultiHash(SessionId, OwnerId);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TOwnerOps
{
    struct TEqual
    {
        bool operator()(const TOwner& l, const TOwner& r) const
        {
            return l.Equal(r);
        }
    };

    struct THash
    {
        size_t operator()(const TOwner& owner) const
        {
            return owner.GetHash();
        }
    };
};

using TOwnedRanges =
    THashMap<TOwner, TLockedRanges, TOwnerOps::THash, TOwnerOps::TEqual>;

struct TLockedNodeInfo
{
    ELockOrigin LockOrigin;
    TOwnedRanges OwnedRanges;
};

struct TOwnedBounds
{
    ui64 OwnerId;
    TRange Range;
};

std::optional<const TOwnedBounds> FindConflictingBounds(
    const TOwnedRanges& ownedRanges,
    const TOwner& owner,
    ui64 min,
    ui64 max,
    ELockMode mode)
{
    for (const auto& [other, ranges]: ownedRanges) {
        if (owner.Equal(other)) {
            continue;
        }
        auto rangeIt = ranges.FindConflictingRange(min, max, mode);
        if (rangeIt != ranges.End()) {
            return TOwnedBounds{other.OwnerId, rangeIt->second};
        }
    }
    return std::nullopt;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TRangeLocks::TImpl
{
    THashMap<ui64, TLockedNodeInfo> LockedNodeInfoByNode;
};

IOutputStream& operator<<(IOutputStream& out, const TLockRange& range)
{
    return out << "{ OwnerId: " << range.OwnerId << ", NodeId: " << range.NodeId
               << ", Offset: " << range.Offset << ", Length: " << range.Length
               << ", Pid: " << range.Pid
               << ", LockMode: " << ToString(range.LockMode)
               << ", LockOrigin: " << ToString(range.LockOrigin) << " }";
}

////////////////////////////////////////////////////////////////////////////////

TRangeLocks::TRangeLocks()
    : Impl(new TImpl())
{}

TRangeLocks::~TRangeLocks() = default;

TRangeLockOperationResult TRangeLocks::Acquire(
    const TString& sessionId,
    ui64 lockId,
    const TLockRange& range)
{
    auto* pointer = Impl->LockedNodeInfoByNode.FindPtr(range.NodeId);
    if (pointer == nullptr) {
        pointer = &Impl->LockedNodeInfoByNode[range.NodeId];
        pointer->LockOrigin = range.LockOrigin;
    } else if (pointer->LockOrigin != range.LockOrigin) {
        if (!pointer->OwnedRanges.empty()) {
            return TRangeLockOperationResult(
                ErrorIncompatibleLockOriginLocks(),
                pointer->LockOrigin);
        }
        pointer->LockOrigin = range.LockOrigin;
    }
    auto& nodeRanges = *pointer;

    auto [min, max] = SafeRange(range.Offset, range.Length);
    auto lock = MakeIntrusive<TLock>(lockId, range.LockMode, range.Pid);

    TOwner owner(sessionId, range.OwnerId);

    auto& ranges = nodeRanges.OwnedRanges[owner];
    TVector<ui64> removedLocks = ranges.Erase(min, max);

    Y_ABORT_UNLESS(ranges.Insert(min, max, std::move(lock)));
    return TRangeLockOperationResult(std::move(removedLocks));
}

TRangeLockOperationResult TRangeLocks::Release(
    const TString& sessionId,
    const TLockRange& range)
{
    auto* node = Impl->LockedNodeInfoByNode.FindPtr(range.NodeId);
    if (node == nullptr) {
        return TRangeLockOperationResult::MakeSucceeded();
    }
    if (range.LockOrigin != node->LockOrigin && !node->OwnedRanges.empty()) {
        return TRangeLockOperationResult(
            ErrorIncompatibleLockOriginLocks(),
            node->LockOrigin);
    }

    TOwner owner(sessionId, range.OwnerId);
    auto* ranges = node->OwnedRanges.FindPtr(owner);
    if (ranges == nullptr) {
        return TRangeLockOperationResult::MakeSucceeded();
    }

    auto [min, max] = SafeRange(range.Offset, range.Length);
    TVector<ui64> removedLocks = ranges->Erase(min, max);
    if (ranges->Empty()) {
        node->OwnedRanges.erase(owner);
    }

    if (node->OwnedRanges.empty()) {
        Impl->LockedNodeInfoByNode.erase(range.NodeId);
    }
    return TRangeLockOperationResult(std::move(removedLocks));
}

TRangeLockOperationResult TRangeLocks::Test(
    const TString& sessionId,
    const TLockRange& range) const
{
    const auto* lockInfo = Impl->LockedNodeInfoByNode.FindPtr(range.NodeId);
    if (lockInfo == nullptr || lockInfo->OwnedRanges.empty()) {
        return TRangeLockOperationResult::MakeSucceeded();
    }

    auto [min, max] = SafeRange(range.Offset, range.Length);
    if (lockInfo->LockOrigin != range.LockOrigin) {
        return TRangeLockOperationResult(
            ErrorIncompatibleLockOriginLocks(),
            lockInfo->LockOrigin);
    }

    TOwner owner(sessionId, range.OwnerId);
    auto ownedBounds = FindConflictingBounds(
        lockInfo->OwnedRanges,
        owner,
        min,
        max,
        range.LockMode);

    if (ownedBounds.has_value()) {
        const auto& ownedRange = ownedBounds->Range;
        Y_ABORT_UNLESS(ownedRange.Lock);

        return TRangeLockOperationResult(
            ErrorIncompatibleLocks(),
            TLockRange{
                .NodeId = range.NodeId,
                .OwnerId = ownedBounds->OwnerId,
                .Offset = ownedRange.Min,
                .Length = ownedRange.Max - ownedRange.Min,
                .Pid = ownedRange.Lock->Pid,
                .LockMode = ownedRange.Lock->Mode,
                .LockOrigin = lockInfo->LockOrigin,
            });
    }
    return TRangeLockOperationResult::MakeSucceeded();
}

////////////////////////////////////////////////////////////////////////////////

TRangeLockOperationResult::TRangeLockOperationResult(
    NProto::TError error,
    TLockIncompatibleInfo incompatible)
    : Error(std::move(error))
    , Value(std::move(incompatible))
{}

TRangeLockOperationResult::TRangeLockOperationResult(
    TVector<ui64> removedLockIds)
    : Error(MakeError(S_OK))
    , Value(std::move(removedLockIds))
{}

TRangeLockOperationResult TRangeLockOperationResult::MakeSucceeded()
{
    return TRangeLockOperationResult();
}

TVector<ui64>& TRangeLockOperationResult::RemovedLockIds()
{
    return std::get<TVector<ui64>>(Value);
}

TLockIncompatibleInfo& TRangeLockOperationResult::Incompatible()
{
    return std::get<TLockIncompatibleInfo>(Value);
}

bool TRangeLockOperationResult::Failed() const
{
    return Error.GetCode() != S_OK;
}

bool TRangeLockOperationResult::Succeeded() const
{
    return Error.GetCode() == S_OK;
}

}   // namespace NCloud::NFileStore::NStorage
