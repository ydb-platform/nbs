#include "ydbauth.h"

#include "config.h"
#include "ydbstats.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/iam/iface/client.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

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
    const TStringType Token;
    const TInstant ExpiresAt;

public:
    TYdbTokenProvider() = default;

    TYdbTokenProvider(TStringType token, TInstant expiresAt)
        : Token(std::move(token))
        , ExpiresAt(expiresAt)
    {
    }

    TStringType GetAuthInfo() const override
    {
        return Token;
    }

    bool IsValid() const override
    {
        return Token && Now() < ExpiresAt;
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
        const auto result = Client->GetToken();
        if (HasError(result)) {
            return std::make_shared<TYdbTokenProvider>();
        }
        const auto tokenInfo = result.GetResult();
        return make_shared<TYdbTokenProvider>(
            tokenInfo.Token,
            tokenInfo.ExpiresAt);
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
