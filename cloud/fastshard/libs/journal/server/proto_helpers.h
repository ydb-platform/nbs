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

// Adds the page groups that |request| asks for to |response|, one content of
// |pageSize| bytes per page, and returns the views of the contents. The
// contents are not initialized. The views are valid until the response is
// changed or destroyed.
TVector<TPageRangeView> MakePageRangeViews(
    const NProto::TReadPagesRequest& request,
    ui32 pageSize,
    NProto::TReadPagesResponse* response);

// The number of pages that |request| asks for, Max<ui64>() if it overflows.
ui64 GetPageCount(const NProto::TReadPagesRequest& request);

TVector<TPageRange> MakePageRanges(const NProto::TWritePagesRequest& request);

TVector<TPageRange> MakePageRanges(
    const NProto::TWriteLogRecordRequest& request);

NProto::TReadJournalTailResponse MakeReadJournalTailResponse(
    const TJournalTail& tail);

}   // namespace NCloud::NJournalled
