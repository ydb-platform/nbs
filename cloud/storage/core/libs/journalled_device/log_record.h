#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>

#include <atomic>
#include <optional>
#include <utility>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct TPageGroupRef
{
    ui64 FirstPageNo = 0;
    ui64 PageCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TLogRecord
{
    ui64 Lsn = 0;
    ui64 PrevLsn = 0;
    TVector<std::pair<ui64, TPageGroupRef>> PageGroupIndex;

    NThreading::TPromise<NCloud::NProto::TError> Promise;
    std::atomic<bool> Ready = false;
};

constexpr ui32 CurrentFormatVersion = 1;

////////////////////////////////////////////////////////////////////////////////

struct TJournalMetadata
{
    ui32 Version = CurrentFormatVersion;
    ui64 LastAckedLsn = 0;
};

TBuffer SerializeMetadata(const TJournalMetadata& metadata);
std::optional<TJournalMetadata> DeserializeMetadata(const TBuffer& buffer);

TBuffer SerializeRecord(const TLogRecord& record);
TLogRecordPtr DeserializeRecord(const TBuffer& buffer);

}   // namespace NCloud::NJournalled
