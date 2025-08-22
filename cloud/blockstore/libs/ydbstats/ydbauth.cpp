#include "ydbauth.h"

#include "config.h"
#include "ydbstats.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/iam/iface/client.h>

#include <contrib/ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_table/table.h>

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

class TYdbTokenProvider
    : public ICredentialsProvider
    , public std::enable_shared_from_this<TYdbTokenProvider>
{
private:
    mutable TMutex Mutex;
    const IIamTokenClientPtr Client;
    mutable TStringType Token;
    mutable TInstant ExpiresAt;
    mutable bool ScheduledGetToken = false;

public:
    TYdbTokenProvider(TTokenInfo initialToken, IIamTokenClientPtr client)
        : Client(std::move(client))
        , Token(std::move(initialToken.Token))
        , ExpiresAt(initialToken.ExpiresAt)
    {}

    TStringType GetAuthInfo() const override
    {
        bool needToScheduleGetToken = false;
        TStringType token;

        with_lock (Mutex) {
            token = Token;
            if (Now() >= ExpiresAt && !ScheduledGetToken) {
                needToScheduleGetToken = true;
                ScheduledGetToken = true;
            }
        }

        if (needToScheduleGetToken) {
            ScheduleGetToken();
        }

        return token;
    }

    bool IsValid() const override
    {
        return true;
    }

private:
    void ScheduleGetToken() const
    {
        auto tokenFuture = Client->GetTokenAsync();

        tokenFuture.Subscribe(
            [weak =
                 weak_from_this()](TFuture<IIamTokenClient::TResponse> future)
            {
                auto self = weak.lock();
                if (!self) {
                    return;
                }

                auto result = future.ExtractValue();

                auto guard = Guard(self->Mutex);

                self->ScheduledGetToken = false;
                if (HasError(result)) {
                    return;
                }

                auto tokenInfo = result.ExtractResult();
                self->Token = std::move(tokenInfo.Token);
                self->ExpiresAt = tokenInfo.ExpiresAt;
            });
    }
};

using TYdbTokenProviderPtr = std::shared_ptr<TYdbTokenProvider>;

////////////////////////////////////////////////////////////////////////////////

class TIamCredentialsProviderFactory final
    : public ICredentialsProviderFactory
{
private:
    TTokenInfo InitialToken;

public:
    TIamCredentialsProviderFactory(TTokenInfo initialToken, IIamTokenClientPtr client)
        : InitialToken(std::move(initialToken))
        , Client(std::move(client))
    {}

    TCredentialsProviderPtr CreateProvider() const {
        return make_shared<TYdbTokenProvider>(InitialToken, Client);
    }

private:
    IIamTokenClientPtr Client;
};

}  // namespace

TCredentialsProviderFactoryPtr CreateIamCredentialsProviderFactory(
    TTokenInfo initialToken,
    IIamTokenClientPtr client)
{
    return std::make_shared<TIamCredentialsProviderFactory>(
        std::move(initialToken),
        std::move(client));
}

}   // namespace NCloud::NBlockStore::NYdbStats
