#pragma once

#include "public.h"

#include "log_record.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

// In-flight records keyed by PrevLsn. They may arrive out of order, so there
// can be gaps; the ready records that follow each other unbroken from
// LastErasedLsn form the chained run, which ends at LastChainedLsn.
class TLogRecordChain
{
private:
    struct TEntry
    {
        bool Ready = false;
        TLogRecordPtr Record;
    };

    mutable TAdaptiveLock Lock;
    ui64 LastErasedLsn = 0;
    ui64 LastChainedLsn = 0;
    THashMap<ui64 /*prevLsn*/, TEntry> Records;

    TLogRecordPtr GetNextImpl(ui64 lsn) const;

public:
    // Call before any Insert; |lsn| is the PrevLsn of the oldest record.
    void InitLastErasedLsn(ui64 lsn);

    // Rejects a record that overlaps a held one or ends at or below the
    // watermark. Returns the record to wait on: the held one for a duplicate.
    TResultOrError<TLogRecordPtr> Insert(TLogRecordPtr record);

    [[nodiscard]] bool MarkAsReady(ui64 prevLsn);
    [[nodiscard]] bool Remove(ui64 prevLsn);

    // Removes and returns the run up to |lsn|, plus the ready records left
    // starting below the new watermark; failing their promises is up to the
    // caller. Fails without changing anything when |lsn| > LastChainedLsn.
    TResultOrError<TVector<TLogRecordPtr>> EraseUpTo(ui64 lsn);

    // The head of the chained run, nullptr when the run is empty.
    TLogRecordPtr GetOldest() const;

    // The ready record following |lsn|, nullptr when missing or not ready.
    TLogRecordPtr GetNext(ui64 lsn) const;

    // Ready records following |afterLsn| up to the first gap or record not
    // ready yet. A zero |maxRecordCount| means no limit.
    TVector<TLogRecordPtr> GetReadyRun(
        ui64 afterLsn,
        ui64 maxRecordCount) const;
};

}   // namespace NCloud::NJournalled
