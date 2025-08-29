#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>

namespace NCloud::NIamClient {

////////////////////////////////////////////////////////////////////////////////

struct TTokenInfo
{
    TString Token;
    TInstant ExpiresAt;

    TTokenInfo() = default;

    TTokenInfo(TString token, TInstant expiresAt)
        : Token(std::move(token))
        , ExpiresAt(expiresAt)
    {}
};

////////////////////////////////////////////////////////////////////////////////

bool IsTokenValid(TInstant now, const TResultOrError<TTokenInfo>& tokenInfo);

////////////////////////////////////////////////////////////////////////////////

struct IIamTokenAsyncClient
    : public IStartable
{
    using TResponse = TResultOrError<TTokenInfo>;

    virtual NThreading::TFuture<TResponse> GetToken() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IIamTokenClient
    : public IStartable
{
    using TResponse = TResultOrError<TTokenInfo>;

    virtual TResponse GetToken() = 0;

    virtual NThreading::TFuture<TResponse> GetTokenAsync() = 0;
};

////////////////////////////////////////////////////////////////////////////////

IIamTokenClientPtr CreateIamTokenClientStub();

}   // namespace NCloud::NIamClient
