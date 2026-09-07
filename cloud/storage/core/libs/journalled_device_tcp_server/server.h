#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/journalled_device/journalled_device.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/threading/future/future.h>

class TNetworkAddress;

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IServerBackend : public IJournalledDevice
{
    virtual ~IServerBackend() = default;

    [[nodiscard]] virtual auto AcquireDevices(
        NProto::TAcquireDevicesRequest request)
        -> NThreading::TFuture<NProto::TAcquireDevicesResponse> = 0;

    [[nodiscard]] virtual auto ReleaseDevices(
        NProto::TReleaseDevicesRequest request)
        -> NThreading::TFuture<NProto::TReleaseDevicesResponse> = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IStartable> CreateServer(
    const TNetworkAddress& listenAddress,
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IServerBackendPtr backend);

}   // namespace NCloud::NJournalled
