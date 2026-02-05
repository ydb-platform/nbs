#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/protos/local_nvme.pb.h>

#include <library/cpp/threading/future/fwd.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ILocalNVMeDeviceProvider
{
    virtual ~ILocalNVMeDeviceProvider() = default;

    [[nodiscard]] virtual auto ListNVMeDevices() const
        -> NThreading::TFuture<TVector<NProto::TNVMeDevice>> = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Creates a provider that reads the NVMe device list from the specified file
ILocalNVMeDeviceProviderPtr CreateFileNVMeDeviceProvider(const TString& path);

ILocalNVMeDeviceProviderPtr CreateLocalNVMeDeviceProviderStub();

}   // namespace NCloud::NBlockStore
