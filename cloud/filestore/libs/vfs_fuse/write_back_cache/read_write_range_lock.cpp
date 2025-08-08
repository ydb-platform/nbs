#include "read_write_range_lock.h"

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

void TReadWriteRangeLock::LockRead(
    ui64 begin,
    ui64 end,
    std::function<void()> action)
{
    Y_ENSURE(
        begin < end,
        "Input argument [" << begin << ", " << end << ") is invalid interval");

    if (WriteLocks.HasIntersection(begin, end)) {
        PendingLocks.push_back({
            .Begin = begin,
            .End = end,
            .Action = std::move(action),
            .IsWrite = false
        });
    } else {
        ReadLocks.AddInterval(begin, end);
        action();
    }
}

void TReadWriteRangeLock::LockWrite(
    ui64 begin,
    ui64 end,
    std::function<void()> action)
{
    Y_ENSURE(
        begin < end,
        "Input argument [" << begin << ", " << end << ") is invalid interval");

    WriteLocks.AddInterval(begin, end);

    if (ReadLocks.HasIntersection(begin, end)) {
        PendingLocks.push_back({
            .Begin = begin,
            .End = end,
            .Action = std::move(action),
            .IsWrite = true
        });
    } else {
        action();
    }
}

void TReadWriteRangeLock::UnlockRead(ui64 begin, ui64 end)
{
    ReadLocks.RemoveInterval(begin, end);
    ProcessPendingLocks();
}

void TReadWriteRangeLock::UnlockWrite(ui64 begin, ui64 end)
{
    WriteLocks.RemoveInterval(begin, end);
    ProcessPendingLocks();
}

bool TReadWriteRangeLock::Empty() const
{
    return ReadLocks.Empty() && WriteLocks.Empty() && PendingLocks.empty();
}

void TReadWriteRangeLock::ProcessPendingLocks()
{
    if (PendingLocks.empty()) {
        return;
    }

    TVector<std::function<void()>> actions;

    EraseIf(PendingLocks, [this, &actions](TPendingLock& pl) {
        if (pl.IsWrite) {
            if (!ReadLocks.HasIntersection(pl.Begin, pl.End)) {
                actions.push_back(std::move(pl.Action));
                return true;
            }
        } else {
            if (!WriteLocks.HasIntersection(pl.Begin, pl.End)) {
                ReadLocks.AddInterval(pl.Begin, pl.End);
                actions.push_back(std::move(pl.Action));
                return true;
            }
        }
        return false;
    });

    for (auto& action: actions) {
        action();
    }
}

}   // namespace NCloud::NFileStore::NFuse
