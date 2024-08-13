#pragma once

#include "public.h"

#include "spec.h"

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NNvme {

////////////////////////////////////////////////////////////////////////////////

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
};

////////////////////////////////////////////////////////////////////////////////

INvmeManagerPtr CreateNvmeManager(TDuration timeout);

}   // namespace NCloud::NBlockStore::NNvme
