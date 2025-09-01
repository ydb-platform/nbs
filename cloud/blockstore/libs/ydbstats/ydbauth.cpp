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
#include <util/system/mutex.h>

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

    const TDuration RefreshTimeBeforeExpiration;

    mutable TMutex Mutex;
    mutable TStringType Token;
    mutable TInstant ExpiresAt;
    mutable std::atomic_bool GetTokenInProgress = false;

public:
    TYdbTokenProvider(
            ISchedulerPtr scheduler,
            IIamTokenClientPtr client,
            TDuration refreshTimeBeforeExpiration,
            TTokenInfo initialToken)
        : Scheduler(std::move(scheduler))
        , Client(std::move(client))
        , RefreshTimeBeforeExpiration(refreshTimeBeforeExpiration)
        , Token(std::move(initialToken.Token))
        , ExpiresAt(initialToken.ExpiresAt)
    {}

    TStringType GetAuthInfo() const override
    {
        bool shouldGetToken = false;
        TStringType token;

        with_lock (Mutex) {
            token = Token;
            if (Now() >= ExpiresAt) {
                shouldGetToken = true;
            }
        }

        if (shouldGetToken) {
            GetTokenIfNeeded();
        }

        return token;
    }

    bool IsValid() const override
    {
        return true;
    }

    void Init()
    {
        ScheduleGetToken();
    }

private:
    // Not thread-safe.
    void ScheduleGetToken() const
    {
        auto deadline = Max(ExpiresAt - RefreshTimeBeforeExpiration, Now());
        Scheduler->Schedule(
            deadline,
            [weak = weak_from_this()]
            {
                auto self = weak.lock();
                if (self) {
                    self->GetTokenIfNeeded();
                }
            });
    }

    void GetTokenIfNeeded() const
    {
        auto inProgress = GetTokenInProgress.exchange(true);
        if (inProgress) {
            return;
        }

        auto tokenFuture = Client->GetTokenAsync();

        tokenFuture.Subscribe(
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
        auto guard = Guard(Mutex);

        Y_DEFER
        {
            GetTokenInProgress.store(false);
            ScheduleGetToken();
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

class TIamCredentialsProviderFactory final: public ICredentialsProviderFactory
{
private:
    const TDuration RefreshTimeBeforeExpiration;
    const TTokenInfo InitialTokenInfo;
    const ISchedulerPtr Scheduler;
    const IIamTokenClientPtr Client;

public:
    TIamCredentialsProviderFactory(
            TDuration refreshTimeBeforeExpiration,
            TTokenInfo initialTokenInfo,
            ISchedulerPtr scheduler,
            IIamTokenClientPtr client)
        : RefreshTimeBeforeExpiration(refreshTimeBeforeExpiration)
        , InitialTokenInfo(std::move(initialTokenInfo))
        , Scheduler(std::move(scheduler))
        , Client(std::move(client))
    {}

    TCredentialsProviderPtr CreateProvider() const
    {
        auto res = make_shared<TYdbTokenProvider>(
            Scheduler,
            Client,
            RefreshTimeBeforeExpiration,
            InitialTokenInfo);
        res->Init();
        return res;
    }
};

}  // namespace

TCredentialsProviderFactoryPtr CreateIamCredentialsProviderFactory(
    TDuration refreshTimeBeforeExpiration,
    TTokenInfo initialTokenInfo,
    ISchedulerPtr scheduler,
    IIamTokenClientPtr client)
{
    return std::make_shared<TIamCredentialsProviderFactory>(
        refreshTimeBeforeExpiration,
        std::move(initialTokenInfo),
        std::move(scheduler),
        std::move(client));
}

}   // namespace NCloud::NBlockStore::NYdbStats
