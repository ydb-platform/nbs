#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <library/cpp/threading/future/fwd.h>

#include <util/generic/fwd.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ILocalNVMeService
    : public NCloud::IStartable
{
    [[nodiscard]] virtual auto GetNVMeDevices() const
        -> TVector<NProto::TNVMeDevice> = 0;

    [[nodiscard]] virtual auto AcquireNVMeDevice(
        const TString& serialNumber) const
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;

    [[nodiscard]] virtual auto ReleaseNVMeDevice(
        const TString& serialNumber) const
        -> NThreading::TFuture<NCloud::NProto::TError> = 0;
};

////////////////////////////////////////////////////////////////////////////////

ILocalNVMeServicePtr CreateLocalNVMeService(
    TLocalNVMeConfigPtr config,
    ILoggingServicePtr logging);

ILocalNVMeServicePtr CreateLocalNVMeServiceStub(ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore
