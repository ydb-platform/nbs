#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/journalled/iface/device.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/vector.h>

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

    NThreading::TPromise<NCloud::NProto::TError> Promise;
};

}   // namespace NCloud::NJournalled
