#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/threading/future/future.h>

class TNetworkAddress;

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IServerBackend
{
    virtual ~IServerBackend() = default;

    [[nodiscard]] virtual auto AcquireDevices(
        TInstant now,
        NProto::TAcquireDevicesRequest request)
        -> NThreading::TFuture<NProto::TAcquireDevicesResponse> = 0;

    [[nodiscard]] virtual auto ReleaseDevices(
        TInstant now,
        NProto::TReleaseDevicesRequest request)
        -> NThreading::TFuture<NProto::TReleaseDevicesResponse> = 0;

    [[nodiscard]] virtual auto ReadPages(
        TInstant now,
        NProto::TReadPagesRequest request)
        -> NThreading::TFuture<NProto::TReadPagesResponse> = 0;

    [[nodiscard]] virtual auto WriteLogRecord(
        TInstant now,
        NProto::TWriteLogRecordRequest request)
        -> NThreading::TFuture<NProto::TWriteLogRecordResponse> = 0;

    [[nodiscard]] virtual auto ReadJournalTail(
        TInstant now,
        NProto::TReadJournalTailRequest request)
        -> NThreading::TFuture<NProto::TReadJournalTailResponse> = 0;

    [[nodiscard]] virtual auto AdvanceLsnLowWatermark(
        TInstant now,
        NProto::TAdvanceLsnLowWatermarkRequest request)
        -> NThreading::TFuture<NProto::TAdvanceLsnLowWatermarkResponse> = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IStartable> CreateServer(
    const TNetworkAddress& listenAddress,
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IServerBackendPtr backend);

}   // namespace NCloud::NJournalled
