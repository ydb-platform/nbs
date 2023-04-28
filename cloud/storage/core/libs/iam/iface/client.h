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
    const TString Token;
    const TInstant ExpiresAt;

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

    // ignoreCached flag makes client not to use possibly cached token.
    virtual TResponse GetToken(bool ignoreCached = false) = 0;

    virtual NThreading::TFuture<TResponse> GetTokenAsync(
        bool ignoreCached = false) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IIamTokenClientPtr CreateIamTokenClientStub();

}   // namespace NCloud::NIamClient
