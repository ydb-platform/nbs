#pragma once

#include <util/generic/map.h>
#include <util/system/spinlock.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

// A monotonically advancing lsn with reference-counted barriers.
class TLsnBarrier
{
private:
    mutable TAdaptiveLock Lock;
    ui64 CurrentLsn = 0;
    TMap<ui64, ui64> BarrierCountByLsn;

public:
    class TGuard
    {
        friend class TLsnBarrier;

    private:
        TLsnBarrier* Barrier;
        const ui64 Lsn;

        TGuard(TLsnBarrier* barrier, ui64 lsn);

    public:
        TGuard(TGuard&& rhs) noexcept;
        TGuard(const TGuard& rhs) = delete;
        TGuard& operator=(const TGuard& rhs) = delete;
        TGuard& operator=(TGuard&& rhs) = delete;

        ~TGuard();

        ui64 GetLsn() const;

        void Release();
    };

public:
    void Advance(ui64 lsn);

    [[nodiscard]] TGuard Acquire();
    [[nodiscard]] TGuard AcquireAtLeast(ui64 lsn);

    ui64 GetBarrierLsn() const;

private:
    void Release(ui64 lsn);
};

}   // namespace NCloud::NJournalled
