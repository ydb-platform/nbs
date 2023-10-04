#include "range_locks.h"

#include <util/digest/multi.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>

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
        return { offset, offset + length };
    } else {
        return { offset, MaxOffset };
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TLock : TSimpleRefCount<TLock>
{
    const ui64 LockId;
    const ELockMode Mode;

    TLock(ui64 lockId, ELockMode mode)
        : LockId(lockId)
        , Mode(mode)
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

struct TRangeOps
{
    struct TCompare
    {
        using is_transparent = void;

        template <typename T1, typename T2>
        bool operator ()(const T1& l, const T2& r) const
        {
            return GetMax(l) < GetMax(r);
        }
    };

    static ui64 GetMax(const TRange& range)
    {
        return range.Max;
    }

    static ui64 GetMax(ui64 offset)
    {
        return offset;
    }
};

using TRangeMap = TSet<TRange, TRangeOps::TCompare>;

////////////////////////////////////////////////////////////////////////////////

class TLockedRanges
{
private:
    TRangeMap Ranges;

public:
    bool Insert(ui64 min, ui64 max, TLockPtr lock)
    {
        return Ranges.emplace(min, max, std::move(lock)).second;
    }

    void Erase(ui64 min, ui64 max, TVector<ui64>& removedLocks)
    {
        auto it = Ranges.upper_bound(min);
        if (it != Ranges.end()) {
            if (it->Min < min) {
                // split range
                bool inserted = Ranges.emplace(it->Min, min, it->Lock).second;
                Y_VERIFY(inserted);

                // trim left range
                const_cast<TRange&>(*it).Min = min;
            }

            // delete ranges
            while (it != Ranges.end() && max >= it->Max) {
                if (it->Lock.RefCount() == 1) {
                    removedLocks.push_back(it->Lock->LockId);
                }
                it = Ranges.erase(it);
            }

            // trim right range
            if (it != Ranges.end() && it->Min < max) {
                const_cast<TRange&>(*it).Min = max;
            }
        }
    }

    bool Test(ui64 min, ui64 max, ELockMode mode, std::pair<ui64, ui64>* range) const
    {
        auto it = Ranges.upper_bound(min);
        while (it != Ranges.end() && it->Min < max) {
            if (!it->Lock->IsCompatible(mode)) {
                if (range) {
                    *range = std::make_pair(it->Min, it->Max);
                }

                return false;
            }
            ++it;
        }

        return true;
    }

    bool Empty() const
    {
        return Ranges.empty();
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
        return SessionId == other.SessionId
            && OwnerId == other.OwnerId;
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
        bool operator ()(const TOwner& l, const TOwner& r) const
        {
            return l.Equal(r);
        }
    };

    struct THash
    {
        size_t operator ()(const TOwner& owner) const
        {
            return owner.GetHash();
        }
    };
};

using TOwnedRanges = THashMap<TOwner, TLockedRanges, TOwnerOps::THash, TOwnerOps::TEqual>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TRangeLocks::TImpl
{
    THashMap<ui64, TOwnedRanges> OwnedRangesByNode;
};

////////////////////////////////////////////////////////////////////////////////

TRangeLocks::TRangeLocks()
    : Impl(new TImpl())
{}

TRangeLocks::~TRangeLocks()
{}

bool TRangeLocks::Acquire(
    const TString& sessionId,
    ui64 lockId,
    TLockRange range,
    ELockMode mode,
    TVector<ui64>& removedLocks)
{
    auto [min, max] = SafeRange(range.Offset, range.Length);
    auto lock = MakeIntrusive<TLock>(lockId, mode);

    TOwner owner(sessionId, range.OwnerId);

    auto& ranges = Impl->OwnedRangesByNode[range.NodeId][owner];
    ranges.Erase(min, max, removedLocks);

    return ranges.Insert(min, max, std::move(lock));
}

void TRangeLocks::Release(
    const TString& sessionId,
    TLockRange range,
    TVector<ui64>& removedLocks)
{
    auto [min, max] = SafeRange(range.Offset, range.Length);

    TOwner owner(sessionId, range.OwnerId);

    auto node = Impl->OwnedRangesByNode.find(range.NodeId);
    if (node == Impl->OwnedRangesByNode.end()) {
        return;
    }

    auto ranges = node->second.find(owner);
    if (ranges == node->second.end()) {
        return;
    }

    ranges->second.Erase(min, max, removedLocks);
    if (ranges->second.Empty()) {
        node->second.erase(ranges);
    }

    if (node->second.empty()) {
        Impl->OwnedRangesByNode.erase(node);
    }
}

bool TRangeLocks::Test(
    const TString& sessionId,
    TLockRange range,
    ELockMode mode,
    TLockRange* conflicting) const
{
    auto [min, max] = SafeRange(range.Offset, range.Length);

    TOwner owner(sessionId, range.OwnerId);
    if (const auto* ownedRanges = Impl->OwnedRangesByNode.FindPtr(range.NodeId)) {
        for (const auto& [other, ranges]: *ownedRanges) {
            std::pair<ui64, ui64> locked = {};
            if (!owner.Equal(other) && !ranges.Test(min, max, mode, &locked)) {
                if (conflicting) {
                    *conflicting = {
                        .NodeId = range.NodeId,
                        .OwnerId = other.OwnerId,
                        .Offset = locked.first,
                        .Length = locked.second - locked.first,
                    };
                }

                return false;
            }
        }
    }

    return true;
}

}   // namespace NCloud::NFileStore::NStorage
