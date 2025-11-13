#pragma once

#include "public.h"

#include "spec.h"

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NNvme {

////////////////////////////////////////////////////////////////////////////////

struct TControllerData
{
    TString DevicePath;

    TString SerialNumber;

    TString ModelNumber;

    ui64 Capacity = 0;
};

struct TPCIAddress
{
    ui16 VendorId = 0;
    ui16 DeviceId = 0;
    TString Address;

    bool operator == (const TPCIAddress&) const = default;
    [[nodiscard]] explicit operator bool () const
    {
        return VendorId != 0 && DeviceId != 0 && !Address.empty();
    }
};

struct INvmeManager
{
    virtual ~INvmeManager() = default;

    virtual NThreading::TFuture<NProto::TError> Format(
        const TString& path,
        nvme_secure_erase_setting ses) = 0;

    virtual NThreading::TFuture<NProto::TError> Deallocate(
        const TString& path,
        ui64 offsetBytes,
        ui64 sizeBytes) = 0;

    virtual TResultOrError<bool> IsSsd(const TString& path) = 0;

    virtual TResultOrError<TString> GetSerialNumber(const TString& path) = 0;
    virtual TResultOrError<TVector<TControllerData>> ListControllers() = 0;

    virtual TResultOrError<TPCIAddress> GetPCIAddress(
        const TString& devicePath) = 0;

    virtual TResultOrError<TString> GetDriverName(const TPCIAddress& pci) = 0;

    virtual NProto::TError BindToVFIO(const TPCIAddress& pci) = 0;
    virtual NProto::TError BindToNVME(const TPCIAddress& pci) = 0;
};

////////////////////////////////////////////////////////////////////////////////

INvmeManagerPtr CreateNvmeManager(TDuration timeout);

}   // namespace NCloud::NBlockStore::NNvme
