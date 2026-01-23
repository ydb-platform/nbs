#pragma once

#include "write_back_cache.h"
#include "write_data_request.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/core/helpers.h>

#include <cloud/storage/core/libs/common/disjoint_interval_map.h>

#include <util/datetime/base.h>
#include <util/generic/intrlist.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCache::TWriteDataEntryPart
{
    const NWriteBackCache::TCachedWriteDataRequest* Source = nullptr;
    ui64 OffsetInSource = 0;
    ui64 Offset = 0;
    ui64 Length = 0;

    ui64 GetEnd() const
    {
        return Offset + Length;
    }

    bool operator==(const TWriteDataEntryPart& p) const
    {
        return std::tie(Source, OffsetInSource, Offset, Length) ==
               std::tie(p.Source, p.OffsetInSource, p.Offset, p.Length);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TUtil
{
public:
    /**
     * Finds cached data from the sequence of write operations for the specified
     *   interval.
     *
     * @param map The interval map of cached write operations.
     * @param startingFromOffset The starting offset of the interval to read.
     * @param length The length of the interval to read.
     * @return A sorted vector of non-overlapping intervals representing cached
     *   data.
     */
    static TVector<TWriteDataEntryPart> CalculateDataPartsToRead(
        const TWriteDataEntryIntervalMap& map,
        ui64 startingFromOffset,
        ui64 length);

    /**
     * Calculate a set of intervals that is complimentary to the input set of
     * intervals for the specified range.
     *
     * @param sortedParts A sorted vector of non-overlapping intervals.
     * @param startingFromOffset The starting offset of the interval to invert
     *   within.
     * @param length The length of the interval to invert within.
     * @return A vector of intervals representing the inverted (missing) parts
     *   within the specified range. Only fields Offset and Length are set.
     *
     * Note: The interval [startingFromOffset, startingFromOffset + length) may
     *   be narrower than the minimal bounding interval for sortedParts.
     *
     * Example:
     *   sortedParts: {{.Offset = 1, .Length = 3}, {.Offset = 5, .Length = 2}}
     *   startingFromOffset: 0
     *   length: 6
     *   result: {{.Offset = 0, .Length = 1}, {.Offset = 3, .Length = 2}}
     */
    static TVector<TWriteDataEntryPart> InvertDataParts(
        const TVector<TWriteDataEntryPart>& sortedParts,
        ui64 startingFromOffset,
        ui64 length);

    static bool IsSorted(const TVector<TWriteDataEntryPart>& parts);
    static bool IsContiguousSequence(const TVector<TWriteDataEntryPart>& parts);

    static NProto::TError ValidateReadDataRequest(
        const NProto::TReadDataRequest& request,
        const TString& expectedFileSystemId);

    static NProto::TError ValidateWriteDataRequest(
        const NProto::TWriteDataRequest& request,
        const TString& expectedFileSystemId);
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TWriteDataEntryIntervalMap
    : public TDisjointIntervalMap<
          ui64,
          NWriteBackCache::TCachedWriteDataRequest*>
{
private:
    using TBase =
        TDisjointIntervalMap<ui64, NWriteBackCache::TCachedWriteDataRequest*>;

public:
    void Add(NWriteBackCache::TCachedWriteDataRequest* entry);
    void Remove(NWriteBackCache::TCachedWriteDataRequest* entry);
};

}   // namespace NCloud::NFileStore::NFuse
