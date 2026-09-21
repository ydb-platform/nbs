#pragma once

#include <cloud/storage/core/libs/journalled/iface/device.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////
// Conversions between the device protocol messages and the IDevice arguments.

TVector<TPageRangeRef> MakePageRangeRefs(
    const NCloud::NProto::TReadPagesRequest& request);

TVector<TPageRange> MakePageRanges(
    const NCloud::NProto::TWriteLogRecordRequest& request);

TVector<TPageRange> MakePageRanges(
    const NCloud::NProto::TJournalRecord& record);

NCloud::NProto::TReadPagesResponse MakeReadPagesResponse(
    const TVector<TPageRangeRef>& rangeRefs,
    const TVector<TBuffer>& pages);

}   // namespace NCloud::NJournalled
