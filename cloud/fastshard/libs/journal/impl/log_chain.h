#pragma once

#include "public.h"

#include "log_record.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include <mutex>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

// The pool holds the records that are not chained yet, by PrevLsn: they may
// arrive out of order. The chain holds the chained records above the lsn low
// watermark, in lsn order.
class TLogRecordChain
{
private:
    struct TEntry
    {
        bool Ready = false;
        TLogRecordPtr Record;
    };

    mutable std::mutex PoolLock;
    THashMap<ui64 /*prevLsn*/, TEntry> RecordPool;
    TLogRecordPtr LastChainedRecord;
    ui64 LastChainedLsn = 0;

    mutable std::mutex ChainLock;
    TDeque<TLogRecordPtr> RecordChain;

    // used only on the executor thread, don't need a lock
    ui64 LsnLowWatermark = 0;

public:
    void InitLastChainedLsn(ui64 lsn);

    TResultOrError<bool> Insert(TLogRecordPtr record);

    [[nodiscard]] bool MarkAsReady(ui64 prevLsn);
    [[nodiscard]] bool Remove(ui64 prevLsn);

    // TODO: too slow, need to update, or call it rarely
    TVector<TLogRecordPtr> RemoveStrandedRecords();

    TLogRecordPtr ChainNextRecord(ui64 lsn);

    TVector<TLogRecordPtr> ExtractRecords(ui64 lsnLowWatermark);
    ui64 GetLsnLowWatermark() const;

    std::pair<TVector<TLogRecordPtr>, ui64 /* lsnLowWatermark */> GetTail(
        ui64 afterLsn,
        ui32 maxRecordCount) const;
};

}   // namespace NCloud::NJournalled
