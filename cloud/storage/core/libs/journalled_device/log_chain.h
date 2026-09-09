#pragma once

#include "public.h"

#include "log_record.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

class TLogRecordChain
{
private:
    mutable TAdaptiveLock Lock;
    ui64 LastErasedLsn = 0;
    TMap<ui64, TLogRecordPtr> Records;

public:
    void InitLastErasedLsn(ui64 lsn);

    // On success returns the record whose promise the caller should wait on.
    // That is the given record when it has been inserted.
    TResultOrError<TLogRecordPtr> Insert(TLogRecordPtr record);

    // Removes one record and returns it, nullptr if there is no such lsn.
    // Does not move LastErasedLsn.
    TLogRecordPtr Extract(ui64 lsn);

    // Removes everything at or below |lsn| and moves LastErasedLsn up to it.
    // Returns the removed records ordered by lsn.
    TVector<TLogRecordPtr> EraseUpTo(ui64 lsn);

    TLogRecordPtr GetOldest() const;

    // Returns the record chained from |lsn| - the one whose PrevLsn is |lsn| -
    // and nullptr across a gap, even when later records are held.
    TLogRecordPtr GetChainedNext(ui64 lsn) const;

    // Returns the longest unbroken run of ready records starting right after
    // |lsn|, stopping at the first gap or unready record. A zero
    // |maxRecordCount| means no limit.
    TVector<TLogRecordPtr> GetReadyRun(ui64 afterLsn, ui64 maxRecordCount)
        const;
};

}   // namespace NCloud::NJournalled
