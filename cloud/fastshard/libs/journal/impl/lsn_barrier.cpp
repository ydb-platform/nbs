#include "lsn_barrier.h"

#include <util/generic/utility.h>
#include <util/system/yassert.h>

#include <utility>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

TLsnBarrier::TGuard::TGuard(std::shared_ptr<TLsnBarrier> barrier, ui64 lsn)
    : Barrier(std::move(barrier))
    , Lsn(lsn)
{}

TLsnBarrier::TGuard::TGuard(TGuard&& rhs) noexcept
    : Barrier(std::exchange(rhs.Barrier, nullptr))
    , Lsn(rhs.Lsn)
{}

TLsnBarrier::TGuard::~TGuard()
{
    Release();
}

ui64 TLsnBarrier::TGuard::GetLsn() const
{
    return Lsn;
}

void TLsnBarrier::TGuard::Release()
{
    if (Barrier) {
        Barrier->Release(Lsn);
        Barrier = nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TLsnBarrier::Advance(ui64 lsn)
{
    std::lock_guard lock(Lock);

    if (CurrentLsn < lsn) {
        CurrentLsn = lsn;
    }
}

TLsnBarrier::TGuard TLsnBarrier::Acquire()
{
    std::lock_guard lock(Lock);

    auto it = BarrierCountByLsn.emplace(CurrentLsn, 0).first;
    ++it->second;
    return TGuard(shared_from_this(), CurrentLsn);
}

TLsnBarrier::TGuard TLsnBarrier::AcquireAtLeast(ui64 lsn)
{
    std::lock_guard lock(Lock);

    lsn = Max(lsn, CurrentLsn);
    auto it = BarrierCountByLsn.emplace(lsn, 0).first;
    ++it->second;
    return TGuard(shared_from_this(), lsn);
}

void TLsnBarrier::Release(ui64 lsn)
{
    std::lock_guard lock(Lock);

    auto it = BarrierCountByLsn.find(lsn);

    Y_ABORT_UNLESS(
        it != BarrierCountByLsn.end(),
        "releasing a barrier that was never acquired: %lu",
        lsn);

    if (--it->second == 0) {
        BarrierCountByLsn.erase(it);
    }
}

ui64 TLsnBarrier::GetBarrierLsn() const
{
    std::lock_guard lock(Lock);

    if (BarrierCountByLsn.empty()) {
        return CurrentLsn;
    }
    return Min(CurrentLsn, BarrierCountByLsn.begin()->first);
}

}   // namespace NCloud::NJournalled
