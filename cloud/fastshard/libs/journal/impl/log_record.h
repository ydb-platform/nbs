#pragma once

#include "public.h"

#include <cloud/fastshard/libs/journal/iface/types.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>

#include <atomic>
#include <memory>
#include <optional>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 CurrentFormatVersion = 1;

////////////////////////////////////////////////////////////////////////////////

struct TPageMapping
{
    ui64 PageNo = 0;
    TPageRangeRef Location;
};

////////////////////////////////////////////////////////////////////////////////

// The pages that a record keeps in memory. Their size is counted in a counter
// shared with the other records. The size is taken from the counter when the
// pages are kept and is returned when this object is destroyed.
class TCachedPages
{
private:
    TVector<TBuffer> Pages;

    // Not set when no pages are kept.
    std::shared_ptr<std::atomic<ui64>> CachedBytes;

public:
    TCachedPages() = default;

    TCachedPages(TCachedPages&& other) noexcept;
    TCachedPages& operator=(TCachedPages&& other) noexcept;

    ~TCachedPages();

    // Keeps the pages if their size fits into the counter under |maxBytes|.
    // Returns an empty object otherwise. Can be called from several threads
    // with the same counter.
    static TCachedPages Reserve(
        TVector<TBuffer> pages,
        std::shared_ptr<std::atomic<ui64>> cachedBytes,
        ui64 maxBytes);

    // The same, but keeps copies of the pages. Makes no copy if the pages do
    // not fit.
    static TCachedPages Reserve(
        const TVector<TStringBuf>& pages,
        std::shared_ptr<std::atomic<ui64>> cachedBytes,
        ui64 maxBytes);

    bool Empty() const
    {
        return Pages.empty();
    }

    const TVector<TBuffer>& Get() const
    {
        return Pages;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TLogRecord
{
    ui64 Lsn = 0;
    ui64 PrevLsn = 0;
    TVector<TPageMapping> PageMappings;
    ui32 PagesCrc = 0;

    // The content of the pages, one buffer per page in the order of the
    // mappings. Empty when the journal does not keep the pages of the record
    // in memory. Set before the record becomes ready and never changed after.
    TCachedPages Pages;

    NThreading::TPromise<NProto::TError> Promise;
};

////////////////////////////////////////////////////////////////////////////////

struct TJournalMetadata
{
    ui32 Version = CurrentFormatVersion;
    ui64 LsnLowWatermark = 0;
};

////////////////////////////////////////////////////////////////////////////////

TBuffer SerializeRecord(const TLogRecord& record);
TLogRecordPtr DeserializeRecord(const TBuffer& buffer);

TBuffer SerializeMetadata(const TJournalMetadata& metadata);
std::optional<TJournalMetadata> DeserializeMetadata(const TBuffer& buffer);

ui32 CalculatePagesCrc(const TVector<TBuffer>& pages);
ui32 CalculatePagesCrc(const TVector<TStringBuf>& pages);

ui32 CalculateMinRecordBufferSize();

}   // namespace NCloud::NJournalled
