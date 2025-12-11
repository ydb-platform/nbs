#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IComputeClient: public IStartable
{
    using TResponse = TResultOrError<TString>;

    virtual NThreading::TFuture<TResponse> CreateTokenForDEK(
        const TString& diskId,
        const TString& taskId,
        const TString& authToken) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IComputeClientPtr CreateComputeClientStub();

}   // namespace NCloud::NBlockStore
