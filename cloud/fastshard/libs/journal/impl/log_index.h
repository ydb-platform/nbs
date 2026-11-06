#pragma once

#include "public.h"

#include "log_record.h"

#include <util/generic/map.h>
#include <util/generic/vector.h>

#include <mutex>
#include <utility>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

class TLogPageIndex
{
private:
    struct TEntry
    {
        ui64 Lsn = 0;
        TPageRangeRef Location;
    };

    using TEntries = TMap<ui64 /*pageNo*/, TEntry>;

    mutable std::mutex Lock;
    ui64 LastIndexedLsn = 0;
    TEntries Entries;

public:
    struct TLookupResult
    {
        ui64 LastIndexedLsn = 0;
        TVector<TPageMapping> Mappings;
    };

    void InitLastIndexedLsn(ui64 lastIndexedLsn);

    // Applies the record continuing the chain from LastIndexedLsn,
    // overwriting the pages it maps and moving LastIndexedLsn up to the
    // record's lsn. Returns false and changes nothing when the record does
    // not chain from it.
    [[nodiscard]] bool TryApplyNext(const TLogRecord& record);

    // Removes the mappings written below |lsn|. Does not move
    // LastIndexedLsn.
    void EraseBelow(ui64 lsn);

    ui64 GetLastIndexedLsn() const;

    // Returns the mappings for |ranges| written after |afterLsn|, clipped to
    // the requested ranges, and the LastIndexedLsn they were read at.
    TLookupResult Lookup(
        const TVector<TPageRangeRef>& ranges,
        ui64 afterLsn) const;

private:
    TEntries::iterator ClearRange(ui64 from, ui64 to);
};

}   // namespace NCloud::NJournalled
