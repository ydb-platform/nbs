#include "ydbstorage.h"

#include "config.h"
#include "ydbauth.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

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

namespace NCloud::NBlockStore::NYdbStats {

using namespace NIamClient;

using namespace NThreading;

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString LoadToken(const TString& path)
{
    try {
        return TFileInput{path}.ReadLine();
    } catch(...) {
        return {};
    }
}

template <typename TResult>
TStatus ExtractStatus(const TFuture<TResult>& future)
{
    try {
        if constexpr (std::is_same<TResult, TStatus>::value) {
            return future.GetValue();
        } else {
            return TStatus{future.GetValue()};
        }
    } catch (...) {
        return TStatus(EStatus::STATUS_UNDEFINED, {});
    }
}

TString ExtractYdbError(const TStatus& status)
{
    TStringStream out;
    status.GetIssues().PrintTo(out, true);
    return out.Str();
}

TString BuildError(
    const TStatus& status,
    const TString& errorPrefix,
    const TString& text)
{
    return TStringBuilder()
        << errorPrefix << text
        << " error code: " << status.GetStatus()
        << " with reason:" << ExtractYdbError(status);
}

TString BuildError(
    const TStatus& status,
    const TString& text)
{
    return TStringBuilder()
        << text
        << " error code: " << status.GetStatus()
        << " with reason:" << ExtractYdbError(status);
}

TDriverConfig BuildDriverConfig(
    TTokenInfo initialTokenInfo,
    const TYdbStatsConfig& config,
    ISchedulerPtr scheduler,
    IIamTokenClientPtr tokenClient)
{
    auto driverConfig = TDriverConfig()
        .SetEndpoint(config.GetServerAddress())
        .SetDatabase(config.GetDatabaseName());

    if (config.GetUseSsl()) {
        driverConfig.SetCredentialsProviderFactory(
            CreateIamCredentialsProviderFactory(
                config.GetIamTokenRefreshTimeBeforeExpiration(),
                std::move(initialTokenInfo),
                std::move(scheduler),
                std::move(tokenClient)));
        driverConfig.UseSecureConnection();
    } else {
       driverConfig.SetAuthToken(LoadToken(config.GetTokenFile()));
    }
    return driverConfig;
}

////////////////////////////////////////////////////////////////////////////////

class TYdbNativeStorage final
    : public IYdbStorage
    , public std::enable_shared_from_this<TYdbNativeStorage>
{
private:
    const TYdbStatsConfigPtr Config;
    const ILoggingServicePtr Logging;
    const ISchedulerPtr Scheduler;
    const IIamTokenClientPtr IamClient;

    std::unique_ptr<TDriver> Driver;
    std::shared_ptr<TTableClient> Client;
    std::unique_ptr<TSchemeClient> SchemeClient;

    TLog Log;

    std::atomic_flag IsInitialized = ATOMIC_FLAG_INIT;

public:
    TYdbNativeStorage(
            TYdbStatsConfigPtr config,
            ILoggingServicePtr logging,
            ISchedulerPtr scheduler,
            IIamTokenClientPtr iamClient)
        : Config(std::move(config))
        , Logging(std::move(logging))
        , Scheduler(std::move(scheduler))
        , IamClient(std::move(iamClient))
    {}

    TFuture<NProto::TError> CreateTable(
        const TString& table,
        const TTableDescription& description) override;

    TFuture<NProto::TError> AlterTable(
        const TString& table,
        const TAlterTableSettings& settings) override;

    TFuture<NProto::TError> DropTable(const TString& table) override;

    TFuture<TDescribeTableResponse> DescribeTable(const TString& table) override;

    TFuture<TGetTablesResponse> GetHistoryTables() override;

    TFuture<NProto::TError> ExecuteUploadQuery(
        TString tableName,
        NYdb::TValue data) override;

    void Start() override;

    void StartImpl(TTokenInfo initialToken)
    {
        Driver = std::make_unique<TDriver>(BuildDriverConfig(
            std::move(initialToken),
            *Config,
            Scheduler,
            IamClient));
        Client = std::make_shared<TTableClient>(*Driver);
        SchemeClient = std::make_unique<TSchemeClient>(*Driver);

        IsInitialized.test_and_set();
    }

    void Stop() override
    {
        SchemeClient.reset();
        Client.reset();
        if (Driver) {
            Driver->Stop(true);
            Driver.reset();
        }
    }

private:
    bool Initialized() const
    {
        return IsInitialized.test();
    }

    TMaybe<TInstant> ExtractTableTime(const TString& name) const;
    TString GetFullTableName(const TString& table) const;
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TError> TYdbNativeStorage::CreateTable(
    const TString& table,
    const TTableDescription& description)
{
    if (!Initialized()) {
        return MakeFuture(
            MakeError(E_REJECTED, "YDB stats storage is not initialized yet"));
    }

    auto tableName = GetFullTableName(table);
    auto future = Client->RetryOperation(
        [=] (TSession session) {
            return session.CreateTable(tableName, TTableDescription(description));
        });

    return future.Apply(
        [=, this] (const auto& future) {
            auto status = ExtractStatus(future);
            if (status.IsSuccess()) {
                return MakeError(S_OK);
            }
            auto out = BuildError(status, "unable to create table ", tableName.Quote());
            STORAGE_ERROR(out);
            return MakeError(E_FAIL, out);
        });
}

TFuture<NProto::TError> TYdbNativeStorage::AlterTable(
    const TString& table,
    const TAlterTableSettings& settings)
{
    if (!Initialized()) {
        return MakeFuture(
            MakeError(E_REJECTED, "YDB stats storage is not initialized yet"));
    }

    auto tableName = GetFullTableName(table);
    auto future = Client->RetryOperation(
        [=] (TSession session) {
            return session.AlterTable(tableName, settings);
        });

    return future.Apply(
        [=, this] (const auto& future) {
            auto status = ExtractStatus(future);
            if (status.IsSuccess()) {
                return MakeError(S_OK);
            }
            auto out = BuildError(status, "unable to alter table ", tableName.Quote());
            STORAGE_ERROR(out);
            return MakeError(E_FAIL, out);
        });
}

TFuture<NProto::TError> TYdbNativeStorage::DropTable(const TString& table)
{
    if (!Initialized()) {
        return MakeFuture(
            MakeError(E_REJECTED, "YDB stats storage is not initialized yet"));
    }

    auto tableName = GetFullTableName(table);
    auto future = Client->RetryOperation(
        [=] (TSession session) {
            return session.DropTable(tableName);
        });

    return future.Apply(
        [=, this] (const auto& future) {
            auto status = ExtractStatus(future);
            if (status.IsSuccess()) {
                return MakeError(S_OK);
            }
            auto out = BuildError(status, "unable to drop table ", tableName.Quote());
            STORAGE_ERROR(out);
            return MakeError(E_FAIL, out);
        });
}

TFuture<NYdbStats::TDescribeTableResponse> TYdbNativeStorage::DescribeTable(
    const TString& table)
{
    if (!Initialized()) {
        return MakeFuture(TDescribeTableResponse(
            MakeError(E_REJECTED, "YDB stats storage is not initialized yet")));
    }

    auto result = NewPromise<NYdbStats::TDescribeTableResponse>();
    auto tableName = GetFullTableName(table);

    TTableClient::TOperationFunc describe = [=] (TSession session) {
        return session.DescribeTable(tableName).Apply([=] (const auto& future) mutable {
            if (future.GetValue().IsSuccess()) {
                const auto& description = future.GetValue().GetTableDescription();
                result.SetValue(TDescribeTableResponse(
                    description.GetColumns(),
                    description.GetPrimaryKeyColumns(),
                    description.GetTtlSettings()));
            }
            return MakeFuture<NYdb::TStatus>(future.GetValue());
        });
    };

    Client->RetryOperation(describe).Subscribe([=, this] (const auto& future) mutable {
        auto status = ExtractStatus(future);
        if (status.IsSuccess()) {
            // promise result is already set
            return;
        }
        if (status.GetStatus() == EStatus::SCHEME_ERROR) {
            auto out = BuildError(status, tableName.Quote(), " does not exist");
            STORAGE_ERROR(out);
            auto error = MakeError(E_NOT_FOUND, out);
            result.SetValue(TDescribeTableResponse(error));
        } else {
            auto out = BuildError(status, "unable to describe table ", tableName.Quote());
            STORAGE_ERROR(out);
            auto error = MakeError(E_FAIL, out);
            result.SetValue(TDescribeTableResponse(error));
        }
    });
    return result;
}

TFuture<TGetTablesResponse> TYdbNativeStorage::GetHistoryTables()
{
    if (!Initialized()) {
        return MakeFuture(TGetTablesResponse(
            MakeError(E_REJECTED, "YDB stats storage is not initialized yet")));
    }

    auto database = Config->GetDatabaseName();
    auto future = SchemeClient->ListDirectory(database);

    return future.Apply(
        [=, this] (const auto& future) {
            auto status = ExtractStatus(future);
            if (!status.IsSuccess()) {
                auto out = BuildError(status, "unable to list directory ", database.Quote());
                STORAGE_ERROR(out);
                return TGetTablesResponse(MakeError(E_FAIL, out));
            }

            TVector<TTableStat> out;
            const auto& tables = future.GetValue().GetChildren();
            for (const auto& item: tables) {
                if (item.Type == ESchemeEntryType::Table) {
                    auto creationTime = ExtractTableTime(item.Name);
                    if (creationTime) {
                        out.emplace_back(item.Name, *creationTime);
                    }
                }
            }

            return TGetTablesResponse(std::move(out));
        });
}

TFuture<NProto::TError> TYdbNativeStorage::ExecuteUploadQuery(
    TString tableName,
    NYdb::TValue data)
{
    if (!Initialized()) {
        return MakeFuture(
            MakeError(E_REJECTED, "YDB stats storage is not initialized yet"));
    }

    auto func = [tableName = std::move(tableName), data = std::move(data)] (
        TTableClient& client) mutable
    {
        auto convertUpsertResult = [] (const TAsyncBulkUpsertResult& future) {
            return ExtractStatus(future);
        };
        auto f = client.BulkUpsert(tableName, NYdb::TValue{data});
        return f.Apply(convertUpsertResult);
    };

    return Client->RetryOperation(func).Apply(
        [](const auto& future)
        {
            auto status = ExtractStatus(future);
            if (status.IsSuccess()) {
                return MakeError(S_OK);
            }
            auto out = BuildError(status, "unable to execute BulkUpsert");
            // E_NOT_FOUND is returned to indicate to the client that probabily
            // some of the tables are missing and client needs to create them
            // or update scheme.
            return (
                status.GetStatus() == EStatus::SCHEME_ERROR
                    ? MakeError(E_NOT_FOUND, out)
                    : MakeError(E_FAIL, out));
        });
}

void TYdbNativeStorage::Start()
{
    Log = Logging->CreateLog("BLOCKSTORE_YDBSTATS");

    if (!Config->GetUseSsl()) {
        STORAGE_INFO("Starting YDB Storage without IAM token");
        StartImpl({});
        return;
    }

    IamClient->GetTokenAsync().Subscribe(
        [weak = weak_from_this()](auto futureToken) mutable
        {
            TTokenInfo token;
            auto result = futureToken.ExtractValue();
            if (!HasError(result)) {
                token = result.ExtractResult();
            }

            if (auto self = weak.lock()) {
                self->StartImpl(token);
            }
        });
}

TMaybe<TInstant> TYdbNativeStorage::ExtractTableTime(const TString& name) const
{
    auto prefixLength = Config->GetHistoryTablePrefix().size() + 1;

    if (prefixLength < name.size()) {
        auto date = name.substr(prefixLength);
        time_t t;
        if (!ParseISO8601DateTime(date.data(), t)) {
            return {};
        }
        return TInstant::Seconds(t);
    }
    return {};
}

TString TYdbNativeStorage::GetFullTableName(const TString& table) const
{
    return TStringBuilder()
        << Config->GetDatabaseName()
        << '/'
        << table;
}

}  // namespace

////////////////////////////////////////////////////////////////////////////////

IYdbStoragePtr CreateYdbStorage(
    TYdbStatsConfigPtr config,
    ILoggingServicePtr logging,
    ISchedulerPtr scheduler,
    IIamTokenClientPtr tokenProvider)
{
    return std::make_shared<TYdbNativeStorage>(
        std::move(config),
        std::move(logging),
        std::move(scheduler),
        std::move(tokenProvider));
}

}   // namespace NCloud::NBlockStore::NYdbStats
