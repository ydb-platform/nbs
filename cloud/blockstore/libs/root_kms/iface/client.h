#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TEncryptionKey;

namespace NProto {

class TKmsKey;

}   // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct IRootKmsClient
    : public IStartable
{
    virtual auto Decrypt(const TString& keyId, const TString& ciphertext)
        -> NThreading::TFuture<TResultOrError<TEncryptionKey>> = 0;

    virtual auto GenerateDataEncryptionKey(const TString& keyId)
        -> NThreading::TFuture<TResultOrError<NProto::TKmsKey>> = 0;
};

////////////////////////////////////////////////////////////////////////////////

IRootKmsClientPtr CreateRootKmsClientStub();

}   // namespace NCloud::NBlockStore
