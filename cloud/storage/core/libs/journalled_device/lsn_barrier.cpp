#include "lsn_barrier.h"

#include <util/generic/utility.h>
#include <util/system/yassert.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

TLsnBarrier::TGuard::TGuard(TLsnBarrier* barrier, ui64 lsn)
    : Barrier(barrier)
    , Lsn(lsn)
{}

TLsnBarrier::TGuard::TGuard(TGuard&& rhs) noexcept
    : Barrier(rhs.Barrier)
    , Lsn(rhs.Lsn)
{
    rhs.Barrier = nullptr;
}

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
    with_lock (Lock) {
        if (CurrentLsn < lsn) {
            CurrentLsn = lsn;
        }
    }
}

TLsnBarrier::TGuard TLsnBarrier::Acquire()
{
    with_lock (Lock) {
        auto it = BarrierCountByLsn.emplace(CurrentLsn, 0).first;
        ++it->second;
        return TGuard(this, CurrentLsn);
    }
}

TLsnBarrier::TGuard TLsnBarrier::AcquireAtLeast(ui64 lsn)
{
    with_lock (Lock) {
        lsn = Max(lsn, CurrentLsn);
        auto it = BarrierCountByLsn.emplace(lsn, 0).first;
        ++it->second;
        return TGuard(this, lsn);
    }
}

void TLsnBarrier::Release(ui64 lsn)
{
    with_lock (Lock) {
        auto it = BarrierCountByLsn.find(lsn);

        Y_ABORT_UNLESS(
            it != BarrierCountByLsn.end(),
            "releasing a barrier that was never acquired: %lu",
            lsn);

        if (--it->second == 0) {
            BarrierCountByLsn.erase(it);
        }
    }
}

ui64 TLsnBarrier::GetBarrierLsn() const
{
    with_lock (Lock) {
        if (BarrierCountByLsn.empty()) {
            return CurrentLsn;
        }
        return Min(CurrentLsn, BarrierCountByLsn.begin()->first);
    }
}

}   // namespace NCloud::NJournalled
