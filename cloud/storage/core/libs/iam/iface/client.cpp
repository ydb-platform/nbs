#include "client.h"

namespace NCloud::NIamClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NThreading;

class TIamTokenClientStub final: public IIamTokenClient
{
public:
    TResultOrError<TTokenInfo> GetToken() override
    {
        return TTokenInfo{"", TInstant::Zero()};
    }

    TFuture<TResultOrError<TTokenInfo>> GetTokenAsync() override
    {
        return MakeFuture(TResultOrError(TTokenInfo{"", TInstant::Zero()}));
    }

    void Start() override
    {}

    void Stop() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsTokenValid(TInstant now, const TResultOrError<TTokenInfo>& tokenInfo)
{
    if (HasError(tokenInfo)) {
        return false;
    }
    const auto& result = tokenInfo.GetResult();
    return result.Token && (now < result.ExpiresAt);
}

////////////////////////////////////////////////////////////////////////////////

IIamTokenClientPtr CreateIamTokenClientStub()
{
    return std::make_shared<TIamTokenClientStub>();
}

}   // namespace NCloud::NIamClient
