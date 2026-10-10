#pragma once

#include "public.h"

#include <cloud/fastshard/protos/device.pb.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IJournalledDevice
{
    virtual ~IJournalledDevice() = default;

    [[nodiscard]] virtual NThreading::TFuture<NProto::TError> Start() = 0;
    [[nodiscard]] virtual NThreading::TFuture<NProto::TError> Stop() = 0;

    [[nodiscard]] virtual auto ReadPages(
        NProto::TReadPagesRequest request)
        -> NThreading::TFuture<NProto::TReadPagesResponse> = 0;

    [[nodiscard]] virtual auto WriteLogRecord(
        NProto::TWriteLogRecordRequest request)
        -> NThreading::TFuture<NProto::TWriteLogRecordResponse> = 0;

    [[nodiscard]] virtual auto ReadJournalTail(
        NProto::TReadJournalTailRequest request)
        -> NThreading::TFuture<NProto::TReadJournalTailResponse> = 0;

    [[nodiscard]] virtual auto AdvanceLsnLowWatermark(
        NProto::TAdvanceLsnLowWatermarkRequest request)
        -> NThreading::TFuture<NProto::TAdvanceLsnLowWatermarkResponse> = 0;
};

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDeviceStub();

}   // namespace NCloud::NJournalled
