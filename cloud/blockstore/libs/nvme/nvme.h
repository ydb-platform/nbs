#pragma once

#include "public.h"

#include "spec.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NNvme {

////////////////////////////////////////////////////////////////////////////////

struct TSanitizeStatus
{
    NProto::TError Status;
    double Progress = 0;
};

struct INvmeManager
    : public IStartable
{
    virtual NThreading::TFuture<NProto::TError> Format(
        const TString& path,
        nvme_secure_erase_setting ses) = 0;

    virtual NThreading::TFuture<NProto::TError> Deallocate(
        const TString& path,
        ui64 offsetBytes,
        ui64 sizeBytes) = 0;

    virtual NProto::TError Sanitize(const TString& ctrlPath) = 0;

    virtual TResultOrError<TSanitizeStatus> GetSanitizeStatus(
        const TString& ctrlPath) = 0;

    virtual TResultOrError<bool> IsSsd(const TString& path) = 0;

    virtual TResultOrError<TString> GetSerialNumber(const TString& path) = 0;

    virtual NProto::TError ResetToSingleNamespace(const TString& ctrlPath) = 0;
};

////////////////////////////////////////////////////////////////////////////////

INvmeManagerPtr CreateNvmeManager(
    ILoggingServicePtr logging,
    TDuration timeout);

}   // namespace NCloud::NBlockStore::NNvme
