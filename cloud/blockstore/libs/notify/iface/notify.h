#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <variant>

namespace NCloud::NBlockStore::NNotify {

////////////////////////////////////////////////////////////////////////////////

struct TDiskError
{
    TString DiskId;
};

struct TDiskBackOnline
{
    TString DiskId;
};

////////////////////////////////////////////////////////////////////////////////

using TEvent = std::variant<
    TDiskError,
    TDiskBackOnline>;

struct TNotification
{
    TString CloudId;
    TString FolderId;
    TString UserId;

    TInstant Timestamp;

    TEvent Event;
};

////////////////////////////////////////////////////////////////////////////////

struct IService
    : public IStartable
{
    virtual NThreading::TFuture<NProto::TError> Notify(
        const TNotification& data) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateNullService(ILoggingServicePtr logging);

IServicePtr CreateServiceStub();

}   // namespace NCloud::NBlockStore::NNotify
