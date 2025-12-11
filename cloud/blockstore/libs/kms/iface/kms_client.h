#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IKmsClient: public IStartable
{
    using TResponse = TResultOrError<TString>;

    virtual NThreading::TFuture<TResponse> Decrypt(
        const TString& keyId,
        const TString& ciphertext,
        const TString& authToken) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IKmsClientPtr CreateKmsClientStub();

}   // namespace NCloud::NBlockStore
