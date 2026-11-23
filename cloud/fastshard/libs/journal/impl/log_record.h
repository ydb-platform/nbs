#pragma once

#include "public.h"

#include <cloud/fastshard/libs/journal/iface/types.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>

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

struct TLogRecord
{
    ui64 Lsn = 0;
    ui64 PrevLsn = 0;
    TVector<TPageMapping> PageMappings;
    ui32 PagesCrc = 0;

    // The content of the pages, one buffer per page in the order of the
    // mappings. Empty when the journal does not keep the pages of the record
    // in memory. Set before the record becomes ready and never changed after.
    TVector<TBuffer> Pages;

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
