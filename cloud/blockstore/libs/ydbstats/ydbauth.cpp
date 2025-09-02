#include "ydbauth.h"

#include "config.h"
#include "ydbstats.h"

#include <cloud/blockstore/libs/kikimr/events.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
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
#include <util/system/rwlock.h>

namespace NCloud::NBlockStore::NYdbStats {

using namespace NIamClient;

using namespace NThreading;

using namespace NYdb;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TYdbTokenProvider final
    : public ICredentialsProvider
    , public std::enable_shared_from_this<TYdbTokenProvider>
{
private:
    const ISchedulerPtr Scheduler;
    const IIamTokenClientPtr Client;
    const TDuration IamTokenRefreshTimeBeforeExpiration;

    mutable TRWMutex Mutex;
    mutable TStringType Token;
    mutable TInstant ExpiresAt;

    mutable std::atomic_bool RefreshTokenInProgress = false;

public:
    TYdbTokenProvider(
            ISchedulerPtr scheduler,
            IIamTokenClientPtr client,
            TDuration iamTokenRefreshTimeBeforeExpiration,
            TTokenInfo initialToken)
        : Scheduler(std::move(scheduler))
        , Client(std::move(client))
        , IamTokenRefreshTimeBeforeExpiration(
              iamTokenRefreshTimeBeforeExpiration)
        , Token(std::move(initialToken.Token))
        , ExpiresAt(initialToken.ExpiresAt)
    {}

    TStringType GetAuthInfo() const override
    {
        TReadGuard guard(Mutex);

        return Token;
    }

    bool IsValid() const override
    {
        TReadGuard guard(Mutex);
        return !Token.empty();
    }

    void Init()
    {
        ScheduleRefreshToken();
    }

private:
    // Not thread-safe.
    void ScheduleRefreshToken() const
    {
        auto deadline =
            Max(ExpiresAt - IamTokenRefreshTimeBeforeExpiration, Now());
        Scheduler->Schedule(
            deadline,
            [weak = weak_from_this()]
            {
                if (auto self = weak.lock()) {
                    self->RefreshToken();
                }
            });
    }

    void RefreshToken() const
    {
        auto inProgress = RefreshTokenInProgress.exchange(true);
        if (inProgress) {
            return;
        }

        Client->GetTokenAsync().Subscribe(
            [weak = weak_from_this()](auto future)
            {
                auto self = weak.lock();
                if (!self) {
                    return;
                }

                self->RenewToken(std::move(future));
            });
    }

    void RenewToken(auto future) const
    {
        TWriteGuard guard(Mutex);

        Y_DEFER
        {
            RefreshTokenInProgress.store(false);
            ScheduleRefreshToken();
        };

        auto result = future.ExtractValue();
        if (HasError(result)) {
            return;
        }

        auto tokenInfo = result.ExtractResult();
        Token = std::move(tokenInfo.Token);
        ExpiresAt = tokenInfo.ExpiresAt;
    }
};

using TYdbTokenProviderPtr = std::shared_ptr<TYdbTokenProvider>;

////////////////////////////////////////////////////////////////////////////////

class TIamCredentialsProviderFactory final
    : public ICredentialsProviderFactory
{
private:
    const TDuration IamTokenRefreshTimeBeforeExpiration;
    const TTokenInfo InitialTokenInfo;
    const ISchedulerPtr Scheduler;
    const IIamTokenClientPtr Client;

public:
    TIamCredentialsProviderFactory(
            TDuration iamTokenRefreshTimeBeforeExpiration,
            TTokenInfo initialTokenInfo,
            ISchedulerPtr scheduler,
            IIamTokenClientPtr client)
        : IamTokenRefreshTimeBeforeExpiration(
              iamTokenRefreshTimeBeforeExpiration)
        , InitialTokenInfo(std::move(initialTokenInfo))
        , Scheduler(std::move(scheduler))
        , Client(std::move(client))
    {}

    TCredentialsProviderPtr CreateProvider() const
    {
        auto res = make_shared<TYdbTokenProvider>(
            Scheduler,
            Client,
            IamTokenRefreshTimeBeforeExpiration,
            InitialTokenInfo);
        res->Init();
        return res;
    }
};

}  // namespace

TCredentialsProviderFactoryPtr CreateIamCredentialsProviderFactory(
    TDuration iamTokenRefreshTimeBeforeExpiration,
    TTokenInfo initialTokenInfo,
    ISchedulerPtr scheduler,
    IIamTokenClientPtr client)
{
    return std::make_shared<TIamCredentialsProviderFactory>(
        iamTokenRefreshTimeBeforeExpiration,
        std::move(initialTokenInfo),
        std::move(scheduler),
        std::move(client));
}

}   // namespace NCloud::NBlockStore::NYdbStats
