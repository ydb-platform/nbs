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

TVector<TPageRange> MakePageRanges(
    const NProto::TWriteLogRecordRequest& request);

TVector<TPageRange> MakePageRanges(const NProto::TWritePagesRequest& request);

TJournalRecord MakeJournalRecord(const NProto::TWriteLogRecordRequest& request);

NProto::TReadJournalTailResponse MakeReadJournalTailResponse(
    const TJournalTail& tail);

NProto::TReadPagesResponse MakeReadPagesResponse(
    const TVector<TPageRangeRef>& rangeRefs,
    const TVector<TBuffer>& pages);

NProto::TReadPagesResponse MakeReadPagesResponse(
    const TVector<TPageRangeRef>& rangeRefs,
    const TReadPagesResult& result);

}   // namespace NCloud::NJournalled
