#pragma once

#include "public.h"

#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/fwd.h>

#include <util/generic/fwd.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ILocalNVMeService: public IStartable
{
    [[nodiscard]] virtual auto ListNVMeDevices() const -> NThreading::TFuture<
        TResultOrError<TVector<NProto::TNVMeDevice>>> = 0;

    [[nodiscard]] virtual auto AcquireNVMeDevice(const TString& serialNumber)
        -> NThreading::TFuture<NProto::TError> = 0;

    [[nodiscard]] virtual auto ReleaseNVMeDevice(const TString& serialNumber)
        -> NThreading::TFuture<NProto::TError> = 0;
};

////////////////////////////////////////////////////////////////////////////////

ILocalNVMeServicePtr CreateLocalNVMeServiceStub();

ILocalNVMeServicePtr CreateLocalNVMeService(
    ILoggingServicePtr logging,
    ILocalNVMeDeviceProviderPtr deviceProvider,
    NNvme::INvmeManagerPtr nvmeManager,
    TExecutorPtr executor);

}   // namespace NCloud::NBlockStore
