#include "ydbstorage.h"

#include "config.h"
#include "ydbauth.h"
#include "ydbstats.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

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
        return future.GetValue();
    } catch (...) {
        return TStatus(EStatus::STATUS_UNDEFINED, {});
    }
}

TString ExtractYdbError(const TStatus& status)
{
    TStringStream out;
    status.GetIssues().PrintTo(out);
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
        << " with reason:\n" << ExtractYdbError(status);
}

////////////////////////////////////////////////////////////////////////////////

TDriverConfig BuildDriverConfig(
    const TYdbStatsConfig& config,
    IIamTokenClientPtr tokenClient)
{
    auto driverConfig = TDriverConfig()
        .SetEndpoint(config.GetServerAddress())
        .SetDatabase(config.GetDatabaseName());

    if (config.GetUseSsl()) {
        driverConfig.SetCredentialsProviderFactory(
            CreateIamCredentialsProviderFactory(tokenClient));
        driverConfig.UseSecureConnection();
    } else {
       driverConfig.SetAuthToken(LoadToken(config.GetTokenFile()));
    }
    return driverConfig;
}

////////////////////////////////////////////////////////////////////////////////

class TYdbNativeStorage final
    : public IYdbStorage
{
private:
    const TYdbStatsConfigPtr Config;
    const ILoggingServicePtr Logging;

    TDriver Driver;
    TTableClient Client;
    TSchemeClient SchemeClient;

    TLog Log;

public:
    TYdbNativeStorage(
            TYdbStatsConfigPtr config,
            ILoggingServicePtr logging,
            IIamTokenClientPtr iamClient)
        : Config(std::move(config))
        , Logging(std::move(logging))
        , Driver(BuildDriverConfig(*Config, std::move(iamClient)))
        , Client(Driver)
        , SchemeClient(Driver)
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
        const TString& query,
        TParams params) override;

    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_YDBSTATS");
    }

    void Stop() override
    {
        Driver.Stop(true);
    }

private:
    TMaybe<TInstant> ExtractTableTime(const TString& name) const;
    TString GetFullTableName(const TString& table) const;

    void ExecutePreparedQuery(
        TDataQuery query,
        TParams params,
        TPromise<NProto::TError> response);
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TError> TYdbNativeStorage::CreateTable(
    const TString& table,
    const TTableDescription& description)
{
    auto tableName = GetFullTableName(table);
    auto future = Client.RetryOperation(
        [=] (TSession session) {
            return session.CreateTable(tableName, TTableDescription(description));
        });

    return future.Apply(
        [=] (const auto& future) {
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
    auto tableName = GetFullTableName(table);
    auto future = Client.RetryOperation(
        [=] (TSession session) {
            return session.AlterTable(tableName, settings);
        });

    return future.Apply(
        [=] (const auto& future) {
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
    auto tableName = GetFullTableName(table);
    auto future = Client.RetryOperation(
        [=] (TSession session) {
            return session.DropTable(tableName);
        });

    return future.Apply(
        [=] (const auto& future) {
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
    auto result = NewPromise<NYdbStats::TDescribeTableResponse>();
    auto tableName = GetFullTableName(table);

    TTableClient::TOperationFunc describe = [=] (TSession session) {
        return session.DescribeTable(tableName).Apply([=] (const auto& future) mutable {
            if (future.GetValue().IsSuccess()) {
                const auto& description = future.GetValue().GetTableDescription();
                result.SetValue(TDescribeTableResponse(
                    description.GetColumns(),
                    description.GetPrimaryKeyColumns()));
            }
            return MakeFuture<NYdb::TStatus>(future.GetValue());
        });
    };

    Client.RetryOperation(describe).Subscribe([=] (const auto& future) mutable {
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
    auto database = Config->GetDatabaseName();
    auto future = SchemeClient.ListDirectory(database);

    return future.Apply(
        [=] (const auto& future) {
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

void TYdbNativeStorage::ExecutePreparedQuery(
    TDataQuery query,
    TParams params,
    TPromise<NProto::TError> response)
{
    TString queryText = query.GetText().GetOrElse("");
    auto future = query.Execute(
        NYdb::NTable::TTxControl::BeginTx(
            NYdb::NTable::TTxSettings::SerializableRW()).CommitTx(),
            std::move(params));
    future.Subscribe ([=](const auto& future) mutable {
        auto status = ExtractStatus(future);
        if (status.IsSuccess()) {
            response.SetValue(MakeError(S_OK));
            return;
        }
        auto out = BuildError(status, "unable to execute query\n", queryText.Quote());
        STORAGE_ERROR(out);
        if (status.GetStatus() == EStatus::SCHEME_ERROR) {
            response.SetValue(MakeError(E_NOT_FOUND, out));
        } else {
            response.SetValue(MakeError(E_FAIL, out));
        }
    });
}

TFuture<NProto::TError> TYdbNativeStorage::ExecuteUploadQuery(
    const TString& query,
    TParams params)
{
    auto response = NewPromise<NProto::TError>();
    auto future = Client.RetryOperation(
        [=, params=std::move(params)] (TSession session) {
            return session.PrepareDataQuery(query).Apply([=](const auto& future) mutable {
                auto status = ExtractStatus(future);
                if (status.IsSuccess()) {
                    auto q = future.GetValue().GetQuery();
                    ExecutePreparedQuery(
                        future.GetValue().GetQuery(),
                        std::move(params),
                        response);
                }
                return MakeFuture<NYdb::TStatus>(future.GetValue());
            });
        }).Subscribe([=] (const auto& future) mutable {
            auto status = ExtractStatus(future);
            if (!status.IsSuccess()) {
                auto out = BuildError(status, "unable to prepare query\n", query.Quote());
                STORAGE_ERROR(out);
                response.SetValue(MakeError(E_FAIL, out));
                return;
            }
        });
    return response;
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
    IIamTokenClientPtr tokenProvider)
{
    return std::make_unique<TYdbNativeStorage>(
        std::move(config),
        std::move(logging),
        std::move(tokenProvider));
}

}   // namespace NCloud::NBlockStore::NYdbStats
