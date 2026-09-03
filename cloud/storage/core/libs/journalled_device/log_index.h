#pragma once

#include "public.h"

#include "log_record.h"

#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

#include <utility>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

class TLogPageMap
{
private:
    using TPageIndex =
        TMap<ui64 /*pageNo*/, std::pair<ui64 /*lsn*/, TPageGroupRef>>;

    mutable TAdaptiveLock Lock;
    ui64 LastIndexedLsn = 0;
    TPageIndex Index;

public:
    void InitLastIndexedLsn(ui64 lastIndexedLsn);

    bool AddNext(const TLogRecord& record);
    void EraseTo(ui64 lsn);
    ui64 GetLastIndexedLsn() const;

    auto GetIndex(const TVector<TPageGroupRef>& pages, ui64 afterLsn) const
        -> std::pair<ui64 /*lsn*/, TVector<std::pair<ui64, TPageGroupRef>>>;

private:
    static TPageIndex::iterator TrimOverlaps(
        TPageIndex& index,
        ui64 from,
        ui64 to);
};

}   // namespace NCloud::NJournalled
