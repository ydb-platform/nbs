#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NLogbroker {

////////////////////////////////////////////////////////////////////////////////

struct TMessage
{
    TString Payload;
    ui64 SeqNo;
};

////////////////////////////////////////////////////////////////////////////////

struct IService
    : public IStartable
{
    virtual NThreading::TFuture<NProto::TError> Write(
        TVector<TMessage> messages,
        TInstant now) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateServiceStub();
IServicePtr CreateServiceNull(ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore::NLogbroker
