#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>

#include <optional>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 CurrentFormatVersion = 1;

////////////////////////////////////////////////////////////////////////////////

struct TPageRange
{
    ui64 FirstPageNo = 0;
    ui64 PageCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TPageMapping
{
    ui64 PageNo = 0;
    TPageRange Location;
};

////////////////////////////////////////////////////////////////////////////////

struct TLogRecord
{
    ui64 Lsn = 0;
    ui64 PrevLsn = 0;
    TVector<TPageMapping> PageMappings;

    NThreading::TPromise<NCloud::NProto::TWriteLogRecordResponse> Promise;
};

////////////////////////////////////////////////////////////////////////////////

struct TJournalMetadata
{
    ui32 Version = CurrentFormatVersion;
    ui64 LastAckedLsn = 0;
};

////////////////////////////////////////////////////////////////////////////////

TBuffer SerializeRecord(const TLogRecord& record);
TLogRecordPtr DeserializeRecord(const TBuffer& buffer);

TBuffer SerializeMetadata(const TJournalMetadata& metadata);
std::optional<TJournalMetadata> DeserializeMetadata(const TBuffer& buffer);

}   // namespace NCloud::NJournalled
