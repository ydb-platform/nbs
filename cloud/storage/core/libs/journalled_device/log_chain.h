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

    TResultOrError<TLogRecordPtr> Insert(TLogRecordPtr record);
    TLogRecordPtr Erase(ui64 lsn);
    TVector<TLogRecordPtr> EraseTo(ui64 lsn);

    TLogRecordPtr Front() const;
    TLogRecordPtr GetNext(ui64 lsn) const;

    TVector<TLogRecordPtr> GetReadyTail(ui64 afterLsn, ui64 maxRecordCnt) const;
};

}   // namespace NCloud::NJournalled
