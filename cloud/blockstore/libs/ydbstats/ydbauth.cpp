#include "ydbauth.h"

#include "config.h"
#include "ydbstats.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/iam/iface/client.h>

#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore::NYdbStats {

using namespace NIamClient;

using namespace NThreading;

using namespace NYdb;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TYdbTokenProvider final
    : public ICredentialsProvider
{
private:
    const IIamTokenClientPtr Client;
    mutable std::string Token;
    mutable TInstant ExpiresAt;

public:
    TYdbTokenProvider(IIamTokenClientPtr client)
        : Client(std::move(client))
    {
    }

    std::string GetAuthInfo() const override
    {
        if (Now() >= ExpiresAt) {
            GetToken();
        }
        return Token;
    }

    bool IsValid() const override
    {
        return true;
    }

private:

    void GetToken() const
    {
        auto result = Client->GetToken();
        if (HasError(result)) {
            return;
        }
        auto tokenInfo = result.GetResult();
        Token = std::move(tokenInfo.Token);
        ExpiresAt = tokenInfo.ExpiresAt;
    }
};

using TYdbTokenProviderPtr = std::shared_ptr<TYdbTokenProvider>;

////////////////////////////////////////////////////////////////////////////////

class TIamCredentialsProviderFactory final
    : public ICredentialsProviderFactory
{
public:
    TIamCredentialsProviderFactory(IIamTokenClientPtr client)
        : Client(std::move(client))
    {}

    TCredentialsProviderPtr CreateProvider() const {
        return make_shared<TYdbTokenProvider>(Client);
    }

private:
    IIamTokenClientPtr Client;
};

}  // namespace

TCredentialsProviderFactoryPtr CreateIamCredentialsProviderFactory(
    IIamTokenClientPtr client)
{
    return std::make_shared<TIamCredentialsProviderFactory>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NYdbStats
