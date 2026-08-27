#pragma once

#include "public.h"

#include "spec.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NNvme {

////////////////////////////////////////////////////////////////////////////////

struct TSanitizeStatus
{
    NProto::TError Status;
    double Progress = 0;
};

struct TLockdownConfig
{
    TVector<ui8> AllowedAdminOpcodes;
    TVector<ui8> AllowedSetFeatureIds;
    bool BlockLockdownCommand = false;
};

struct TLockdownScopeState
{
    // Identifier may be prohibited using Command and Feature Lockdown.
    TVector<ui8> Supported;

    // Identifier is currently prohibited.
    TVector<ui8> Prohibited;
};

struct TLockdownState
{
    // Command and Feature Lockdown is supported by the controller.
    bool Supported = false;

    TLockdownScopeState AdminCmd;
    TLockdownScopeState FeatureId;
};

struct INvmeManager: public IStartable
{
    virtual NThreading::TFuture<NProto::TError> Format(
        const TString& path,
        nvme_secure_erase_setting ses) = 0;

    virtual NThreading::TFuture<NProto::TError>
    Deallocate(const TString& path, ui64 offsetBytes, ui64 sizeBytes) = 0;

    virtual NProto::TError StartSanitize(const TString& ctrlPath) = 0;

    virtual TResultOrError<TSanitizeStatus> GetSanitizeStatus(
        const TString& ctrlPath) = 0;

    virtual TResultOrError<bool> IsSsd(const TString& path) = 0;

    virtual TResultOrError<TString> GetSerialNumber(const TString& path) = 0;

    virtual NProto::TError ResetToSingleNamespace(const TString& ctrlPath) = 0;

    virtual TResultOrError<TString> GetDeviceModel(const TString& path) = 0;

    virtual TResultOrError<TLockdownState> GetLockdownState(
        const TString& ctrlPath) = 0;

    virtual NProto::TError EnsureLockdown(
        const TString& ctrlPath,
        const TLockdownConfig& config) = 0;
};

////////////////////////////////////////////////////////////////////////////////

INvmeManagerPtr CreateNvmeManager(
    ILoggingServicePtr logging,
    TDuration secureEraseTimeout,
    TDuration adminCmdTimeout);

}   // namespace NCloud::NBlockStore::NNvme
