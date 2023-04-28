#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>


namespace NCloud::NBlockStore::NNotify {

////////////////////////////////////////////////////////////////////////////////

struct TDiskErrorNotification
{
    TString DiskId;

    TString CloudId;
    TString FolderId;
    TString UserId;
};

////////////////////////////////////////////////////////////////////////////////

struct IService
    : public IStartable
{
    virtual ~IService() = default;

    virtual NThreading::TFuture<NProto::TError> NotifyDiskError(
        const TDiskErrorNotification& data) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateService(TNotifyConfigPtr config);

IServicePtr CreateNullService(ILoggingServicePtr logging);

IServicePtr CreateServiceStub();

}   // namespace NCloud::NBlockStore::NNotify
