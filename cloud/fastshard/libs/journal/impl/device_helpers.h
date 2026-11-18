#pragma once

#include <cloud/fastshard/libs/journal/iface/device.h>
#include <cloud/fastshard/public/api/protos/device.pb.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////
// Conversions between the device protocol messages and the IDevice arguments.

TVector<TPageRangeRef> MakePageRangeRefs(
    const NProto::TReadPagesRequest& request);

TVector<TPageRange> MakePageRanges(
    const NProto::TWriteLogRecordRequest& request);

TVector<TPageRange> MakePageRanges(const NProto::TWritePagesRequest& request);

TVector<TPageRange> MakePageRanges(const NProto::TJournalRecord& record);

NProto::TReadPagesResponse MakeReadPagesResponse(
    const TVector<TPageRangeRef>& rangeRefs,
    const TVector<TBuffer>& pages);

}   // namespace NCloud::NJournalled
