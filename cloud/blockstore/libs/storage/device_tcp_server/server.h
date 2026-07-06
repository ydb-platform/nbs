#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

class TNetworkAddress;

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IDeviceServerBackend
{
    virtual ~IDeviceServerBackend() = default;

    [[nodiscard]] virtual auto AcquireDevices(
        TInstant now,
        NProto::TAcquireDevicesRequest request)
        -> NThreading::TFuture<NProto::TAcquireDevicesResponse> = 0;

    [[nodiscard]] virtual auto ReleaseDevices(
        TInstant now,
        NProto::TReleaseDevicesRequest request)
        -> NThreading::TFuture<NProto::TReleaseDevicesResponse> = 0;

    [[nodiscard]] virtual auto ZeroDeviceBlocks(
        TInstant now,
        NProto::TZeroDeviceBlocksRequest request)
        -> NThreading::TFuture<NProto::TZeroDeviceBlocksResponse> = 0;

    [[nodiscard]] virtual auto ReadDeviceBlocks(
        TInstant now,
        NProto::TReadDeviceBlocksRequest request)
        -> NThreading::TFuture<NProto::TReadDeviceBlocksResponse> = 0;

    [[nodiscard]] virtual auto WriteDeviceBlocks(
        TInstant now,
        NProto::TWriteDeviceBlocksRequest request)
        -> NThreading::TFuture<NProto::TWriteDeviceBlocksResponse> = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IStartable> CreateDeviceTCPServer(
    const TNetworkAddress& listenAddress,
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IDeviceServerBackendPtr backend);

}   // namespace NCloud::NBlockStore::NStorage
