#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/fwd.h>

#include <util/generic/fwd.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ILocalNVMeService: public IStartable
{
    [[nodiscard]] virtual auto ListNVMeDevices() const
        -> TResultOrError<TVector<NProto::TNVMeDevice>> = 0;

    [[nodiscard]] virtual auto AcquireNVMeDevice(
        const TString& serialNumber) const
        -> NThreading::TFuture<NProto::TError> = 0;

    [[nodiscard]] virtual auto ReleaseNVMeDevice(
        const TString& serialNumber) const
        -> NThreading::TFuture<NProto::TError> = 0;
};

////////////////////////////////////////////////////////////////////////////////

ILocalNVMeServicePtr CreateLocalNVMeServiceStub();

}   // namespace NCloud::NBlockStore
