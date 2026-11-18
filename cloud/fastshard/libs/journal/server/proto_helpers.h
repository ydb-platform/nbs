#pragma once

#include <cloud/fastshard/libs/journal/iface/device.h>
#include <cloud/fastshard/libs/journal/iface/journalled_device.h>
#include <cloud/fastshard/public/api/protos/device.pb.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////
// Conversions between the device protocol messages and the arguments of
// IDevice and IJournalledDevice.

TVector<TPageRangeRef> MakePageRangeRefs(
    const NProto::TReadPagesRequest& request);

// The views below point to the pages of the request. The request must live
// as long as they are used.

TVector<TPageRangeView> MakePageRangeViews(
    const NProto::TWritePagesRequest& request);

TVector<TPageRangeView> MakePageRangeViews(
    const NProto::TWriteLogRecordRequest& request);

NProto::TReadJournalTailResponse MakeReadJournalTailResponse(
    const TJournalTail& tail);

NProto::TReadPagesResponse MakeReadPagesResponse(
    const TVector<TPageRangeRef>& rangeRefs,
    const TVector<TBuffer>& pages);

NProto::TReadPagesResponse MakeReadPagesResponse(
    const TVector<TPageRangeRef>& rangeRefs,
    const TReadPagesResult& result);

}   // namespace NCloud::NJournalled
