#pragma once

#include "public.h"

#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IJournalledDevice
{
    virtual ~IJournalledDevice() = default;

    [[nodiscard]] virtual auto ReadPages(
        NCloud::NProto::TReadPagesRequest request)
        -> NThreading::TFuture<NCloud::NProto::TReadPagesResponse> = 0;

    [[nodiscard]] virtual auto WriteLogRecord(
        NCloud::NProto::TWriteLogRecordRequest request)
        -> NThreading::TFuture<NCloud::NProto::TWriteLogRecordResponse> = 0;

    [[nodiscard]] virtual auto ReadJournalTail(
        NCloud::NProto::TReadJournalTailRequest request)
        -> NThreading::TFuture<NCloud::NProto::TReadJournalTailResponse> = 0;

    [[nodiscard]] virtual auto AdvanceLsnLowWatermark(
        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request)
        -> NThreading::TFuture<NCloud::NProto::TAdvanceLsnLowWatermarkResponse> = 0;
};

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDevice(IDevicePtr dataStore);

}   // namespace NCloud::NJournalled
