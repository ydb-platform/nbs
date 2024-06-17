#include "ydbrow.h"
#include "ydbstats.h"

#include "config.h"
#include "ydbstorage.h"
#include "ydbscheme.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NYdbStats {

using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

namespace {

////////////////////////////////////////////////////////////////////////////////

TYdbStatsConfigPtr CreateTestConfig()
{
    NProto::TYdbStatsConfig config;
    config.SetStatsTableName("test");
    config.SetHistoryTablePrefix("test");
    config.SetHistoryTableLifetimeDays(3);
    config.SetStatsTableRotationAfterDays(1);
    config.SetBlobLoadMetricsTableName("metrics");

    return std::make_shared<TYdbStatsConfig>(config);
}

TYdbStatsConfigPtr CreateTestConfig(TDuration statsTtl, TDuration archiveTtl)
{
    NProto::TYdbStatsConfig config;
    config.SetStatsTableName("test");
    config.SetArchiveStatsTableName("arctest");
    config.SetHistoryTablePrefix("test");
    config.SetHistoryTableLifetimeDays(3);
    config.SetStatsTableRotationAfterDays(1);
    config.SetBlobLoadMetricsTableName("metrics");
    config.SetStatsTableTtl(statsTtl.MilliSeconds());
    config.SetArchiveStatsTableTtl(archiveTtl.MilliSeconds());

    return std::make_shared<TYdbStatsConfig>(config);
}

TYdbStatsConfigPtr CreateAllTablesTestConfig()
{
    NProto::TYdbStatsConfig config;
    config.SetStatsTableName("test");
    config.SetHistoryTablePrefix("test");
    config.SetArchiveStatsTableName("archive-test");
    config.SetHistoryTableLifetimeDays(3);
    config.SetStatsTableRotationAfterDays(1);
    config.SetBlobLoadMetricsTableName("metrics");
    config.SetDatabaseName("/Root");

    return std::make_shared<TYdbStatsConfig>(config);
}

TStatsTableSchemePtr CreateStatsTestScheme(TDuration ttl)
{
    TStatsTableSchemeBuilder out;
    static TVector<std::pair<TString, NYdb::EPrimitiveType>> columns = {
        {"DiskId",      NYdb::EPrimitiveType::Utf8  },
        {"Timestamp",   NYdb::EPrimitiveType::Utf8  },
        {"UintField",   NYdb::EPrimitiveType::Uint64},
        {"DoubleField", NYdb::EPrimitiveType::Double}
    };
    out.SetKeyColumns({"DiskId"});
    out.AddColumns(columns);
    if (ttl) {
        out.SetTtl(NYdb::NTable::TTtlSettings{
            "DiskId",
            NTable::TTtlSettings::EUnit::MilliSeconds,
            ttl});
    }
    return out.Finish();
}

TStatsTableSchemePtr CreateStatsTestScheme()
{
    return CreateStatsTestScheme({});
}

TStatsTableSchemePtr CreateArchiveStatsTestScheme()
{
    return CreateStatsTestScheme();
}

TStatsTableSchemePtr CreateArchiveStatsTestScheme(TDuration ttl)
{
    return CreateStatsTestScheme(ttl);
}

TStatsTableSchemePtr CreateMetricsTestScheme()
{
    TStatsTableSchemeBuilder out;
    static TVector<std::pair<TString, NYdb::EPrimitiveType>> columns = {
        {"HostName",       NYdb::EPrimitiveType::String},
        {"Timestamp",      NYdb::EPrimitiveType::Timestamp},
        {"ThroughputData", NYdb::EPrimitiveType::Json},
    };
    out.SetKeyColumns({"DiskId"});
    out.AddColumns(columns);
    return out.Finish();
}

TStatsTableSchemePtr CreateNewStatsTestScheme(TDuration ttl)
{
    TStatsTableSchemeBuilder out;
    static TVector<std::pair<TString, NYdb::EPrimitiveType>> columns = {
        {"DiskId",      NYdb::EPrimitiveType::Utf8  },
        {"Timestamp",   NYdb::EPrimitiveType::Utf8  },
        {"UintField",   NYdb::EPrimitiveType::Uint64},
        {"DoubleField", NYdb::EPrimitiveType::Double},
        {"NewField",    NYdb::EPrimitiveType::Uint64}
    };
    out.SetKeyColumns({"DiskId"});
    out.AddColumns(columns);
    if (ttl) {
        out.SetTtl(NYdb::NTable::TTtlSettings{
            "DiskId",
            NTable::TTtlSettings::EUnit::MilliSeconds,
            ttl});
    }
    return out.Finish();
}

TStatsTableSchemePtr CreateNewStatsTestScheme()
{
    return CreateNewStatsTestScheme({});
}

TStatsTableSchemePtr CreateNewArchiveStatsTestScheme(TDuration ttl)
{
    return CreateNewStatsTestScheme(ttl);
}

TStatsTableSchemePtr CreateNewArchiveStatsTestScheme()
{
    return CreateNewArchiveStatsTestScheme({});
}

TStatsTableSchemePtr CreateBadStatsTestScheme()
{
    TStatsTableSchemeBuilder out;
    static TVector<std::pair<TString, NYdb::EPrimitiveType>> columns = {
        {"DiskId",      NYdb::EPrimitiveType::Utf8  },
        {"Timestamp",   NYdb::EPrimitiveType::Utf8  },
        {"UintField",   NYdb::EPrimitiveType::Double},
        {"DoubleField", NYdb::EPrimitiveType::Double},
        {"NewField",    NYdb::EPrimitiveType::Uint64}
    };
    out.SetKeyColumns({"DiskId"});
    out.AddColumns(columns);
    return out.Finish();
}

TStatsTableSchemePtr CreateBadArchiveStatsTestScheme()
{
    return CreateBadStatsTestScheme();
}

TStatsTableSchemePtr CreateHistoryTestScheme()
{
    TStatsTableSchemeBuilder out;
    static TVector<std::pair<TString, NYdb::EPrimitiveType>> columns = {
        {"DiskId",      NYdb::EPrimitiveType::Utf8  },
        {"Timestamp",   NYdb::EPrimitiveType::Utf8  },
        {"UintField",   NYdb::EPrimitiveType::Uint64},
        {"DoubleField", NYdb::EPrimitiveType::Double}
    };
    out.SetKeyColumns({"DiskId", "Timestamp"});
    out.AddColumns(columns);
    return out.Finish();
}

TStatsTableSchemePtr CreateNewHistoryTestScheme()
{
    TStatsTableSchemeBuilder out;
    static TVector<std::pair<TString, NYdb::EPrimitiveType>> columns = {
        {"DiskId",      NYdb::EPrimitiveType::Utf8  },
        {"Timestamp",   NYdb::EPrimitiveType::Utf8  },
        {"UintField",   NYdb::EPrimitiveType::Uint64},
        {"DoubleField", NYdb::EPrimitiveType::Double},
        {"NewField",    NYdb::EPrimitiveType::Uint64}
    };
    out.SetKeyColumns({"DiskId", "Timestamp"});
    out.AddColumns(columns);
    return out.Finish();
}

TStatsTableSchemePtr CreateBadHistoryTestScheme()
{
    TStatsTableSchemeBuilder out;
    static TVector<std::pair<TString, NYdb::EPrimitiveType>> columns = {
        {"DiskId",      NYdb::EPrimitiveType::Utf8  },
        {"Timestamp",   NYdb::EPrimitiveType::Utf8  },
        {"UintField",   NYdb::EPrimitiveType::Double},
        {"DoubleField", NYdb::EPrimitiveType::Double},
        {"NewField",    NYdb::EPrimitiveType::Uint64}
    };
    out.SetKeyColumns({"DiskId", "Timestamp"});
    out.AddColumns(columns);
    return out.Finish();
}

NYdbStats::TYdbRow BuildTestStats()
{
    TYdbRow out;
    out.DiskId = "vol0";
    out.Timestamp = TInstant::Now().Seconds();
    out.BlocksCount = 100;
    out.BlockSize = 4096;
    return out;
}

NYdbStats::TYdbBlobLoadMetricRow BuildTestMetrics()
{
    TYdbBlobLoadMetricRow out;
    out.HostName = "Host";
    out.Timestamp = TInstant::Now();
    out.LoadData = "{}";
    return out;
}

////////////////////////////////////////////////////////////////////////////////

class TYdbTestStorage final
    : public IYdbStorage
{
private:
    TYdbStatsConfigPtr Config;

    THashMap<TString, TStatsTableSchemePtr> Tables;

public:
    ui32 GetHistoryTablesCalls = 0;
    ui32 DescribeTableCalls = 0;
    ui32 DropTableCalls = 0;
    ui32 CreateTableCalls = 0;
    ui32 AlterTableCalls = 0;
    ui32 UpsertCalls = 0;

    using TUploadCheckFunc =
        std::function<TFuture<NProto::TError>(TString, NYdb::TValue)>;

    TUploadCheckFunc UploadTestFunc;

public:
    TYdbTestStorage(TYdbStatsConfigPtr config)
        : Config(std::move(config))
    {
    }

    void Start() override
    {
    }

    void Stop() override
    {
    }

    void AddTable(const TString& name, TStatsTableSchemePtr scheme)
    {
        Tables[name] = std::move(scheme);
    }

    void AddTables(
        TStatsTableSchemePtr statsTableScheme,
        TStatsTableSchemePtr metricsTableScheme,
        TStatsTableSchemePtr historyTableScheme,
        const TVector<TTableStat>& historyTables,
        bool statTable)
    {
        for (const auto& t : historyTables) {
            Tables.emplace(t.first + "-" + t.second.FormatLocalTime("%F"), historyTableScheme);
        }
        if (statTable) {
            Tables.emplace(Config->GetStatsTableName(), statsTableScheme);
        }
        if (metricsTableScheme) {
            Tables.emplace(Config->GetBlobLoadMetricsTableName(), metricsTableScheme);
        }
    }

    TFuture<TGetTablesResponse> GetHistoryTables() override
    {
        ++GetHistoryTablesCalls;
        TVector<TTableStat> historyTables;
        for (const auto& e : Tables) {
            auto time = ExtractTableTime(e.first);
            if (time.Defined()) {
                historyTables.push_back({e.first, *time});
            }
        }
        return MakeFuture(TGetTablesResponse(historyTables));
    }

    TFuture<TDescribeTableResponse> DescribeTable(const TString& table) override
    {
        ++DescribeTableCalls;
        auto it = Tables.find(table);
        if (it != Tables.end()) {
            TVector<NYdb::TColumn> columns;
            for (const auto& column : it->second->Columns) {
                TTypeBuilder builder;
                builder.Optional(column.Type);
                columns.emplace_back(column.Name, builder.Build());
            }
            return MakeFuture(TDescribeTableResponse(
                std::move(columns),
                it->second->KeyColumns,
                it->second->Ttl));
        } else {
            return MakeFuture(TDescribeTableResponse(MakeError(E_NOT_FOUND, "Table not found")));
        }
    }

    TFuture<NProto::TError> DropTable(const TString& table) override
    {
        ++DropTableCalls;
        auto it = Tables.find(table);
        if (it != Tables.end()) {
            Tables.erase(table);
            return MakeFuture(MakeError(S_OK));
        } else {
            return MakeFuture(MakeError(E_NOT_FOUND, "Table not found"));
        }
    }

    TFuture<NProto::TError> CreateTable(
        const TString& table,
        const NYdb::NTable::TTableDescription& description) override
    {
        ++CreateTableCalls;
        bool inserted;
        auto scheme = std::make_shared<TStatsTableScheme>(
            description.GetColumns(),
            description.GetPrimaryKeyColumns(),
            description.GetTtlSettings());
        std::tie(std::ignore, inserted) = Tables.insert(std::make_pair(table, scheme));
        if (inserted) {
            return MakeFuture(MakeError(S_OK));
        } else {
            return MakeFuture(MakeError(E_REJECTED, "Table already exists"));
        }
    }

    TFuture<NProto::TError> AlterTable(
        const TString& table,
        const NYdb::NTable::TAlterTableSettings& settings) override
    {
        ++AlterTableCalls;
        auto it = Tables.find(table);
        if (it == Tables.end()) {
            return MakeFuture(MakeError(E_NOT_FOUND, "Table not found"));
        } else {
            auto origColumns = it->second->Columns;
            auto origKeyColumns = it->second->KeyColumns;
            for (const auto& c : settings.AddColumns_) {
                auto it = FindIf(
                    origColumns.begin(),
                    origColumns.end(),
                    [&] (const NYdb::TColumn& column) {
                        return c.Name == column.Name;
                    });
                if (it != origColumns.end()) {
                    return MakeFuture(MakeError(E_ARGUMENT, "Duplicated columns found"));
                }
                origColumns.push_back(c);
            }
            Tables.erase(table);
            Tables.insert({
                table,
                std::make_shared<TStatsTableScheme>(
                    origColumns,
                    origKeyColumns,
                    settings.GetAlterTtlSettings().Empty()
                        ? TMaybe<TTtlSettings>{}
                        : settings.GetAlterTtlSettings()->GetTtlSettings())});
            return MakeFuture(MakeError(S_OK));
        }
    }

    TFuture<NProto::TError> ExecuteUploadQuery(
        TString tableName,
        NYdb::TValue data) override
    {
        ++UpsertCalls;
        if (UploadTestFunc) {
            return UploadTestFunc(std::move(tableName), std::move(data));
        }
        return MakeFuture(MakeError(S_OK));
    }

private:
    TMaybe<TInstant> ExtractTableTime(const TString& name)
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
};

using TYdbTestStoragePtr = std::shared_ptr<TYdbTestStorage>;

TYdbTestStoragePtr YdbCreateTestStorage(
    TYdbStatsConfigPtr config)
{
    return std::make_shared<TYdbTestStorage>(std::move(config));
}

}    // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TYdbStatsUploadTest)
{
    Y_UNIT_TEST(ShouldCreateTablesIfNessesary)
    {
        auto config = CreateTestConfig();
        auto statsScheme = CreateStatsTestScheme();
        auto historyScheme = CreateHistoryTestScheme();
        auto archiveScheme = CreateArchiveStatsTestScheme();
        auto metricsScheme = CreateMetricsTestScheme();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            statsScheme,
            historyScheme,
            archiveScheme,
            metricsScheme);
        uploader->Start();

        auto response = uploader->UploadStats(
             { BuildTestStats() },
             { BuildTestMetrics() }).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(3, ydbTestStorage->CreateTableCalls);
    }

    Y_UNIT_TEST(ShouldNotCreateTablesIfTheyAlreadyExistAndNotOutdated)
    {
        auto config = CreateTestConfig();
        auto statsScheme = CreateStatsTestScheme();
        auto historyScheme = CreateHistoryTestScheme();
        auto archiveScheme = CreateArchiveStatsTestScheme();
        auto metricsScheme = CreateMetricsTestScheme();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            statsScheme,
            historyScheme,
            archiveScheme,
            metricsScheme);
        uploader->Start();

        TVector<TTableStat> directory;
        directory.push_back(std::make_pair(TString("test"), TInstant::Now()));
        directory.push_back(std::make_pair(TString("test"), TInstant::Now() - TDuration::Days(5)));
        ydbTestStorage->AddTables(
            statsScheme,
            metricsScheme,
            historyScheme,
            directory,
            true);

        auto response = uploader->UploadStats(
            { BuildTestStats() },
            { BuildTestMetrics() }).GetValueSync();
        UNIT_ASSERT(
            response.GetCode() == S_OK &&
            ydbTestStorage->CreateTableCalls == 0);
    }

    Y_UNIT_TEST(ShouldCreateNewHistoryTableIfItIsOutdated)
    {
        auto config = CreateTestConfig();
        auto statsScheme = CreateStatsTestScheme();
        auto historyScheme = CreateHistoryTestScheme();
        auto archiveScheme = CreateArchiveStatsTestScheme();
        auto metricsScheme = CreateMetricsTestScheme();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            statsScheme,
            historyScheme,
            archiveScheme,
            metricsScheme);
        uploader->Start();

        TVector<TTableStat> directory;
        directory.push_back(std::make_pair(TString("test"), TInstant::Now() - TDuration::Days(5)));
        ydbTestStorage->AddTables(
            statsScheme,
            metricsScheme,
            historyScheme,
            directory,
            true);

        auto response = uploader->UploadStats(
            { BuildTestStats() },
            { BuildTestMetrics() }).GetValueSync();
        UNIT_ASSERT(
            response.GetCode() == S_OK &&
            ydbTestStorage->CreateTableCalls == 1);
    }

    Y_UNIT_TEST(ShouldAlterTables)
    {
        auto config = CreateTestConfig();
        auto statsScheme = CreateStatsTestScheme();
        auto historyScheme = CreateHistoryTestScheme();
        auto statsNewScheme = CreateNewStatsTestScheme();
        auto historyNewScheme = CreateNewHistoryTestScheme();
        auto archiveNewScheme = CreateNewArchiveStatsTestScheme();
        auto metricsScheme = CreateMetricsTestScheme();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            statsNewScheme,
            historyNewScheme,
            archiveNewScheme,
            metricsScheme);
        uploader->Start();

        TVector<TTableStat> directory;
        directory.push_back(std::make_pair(TString("test"), TInstant::Now()));
        ydbTestStorage->AddTables(
            statsScheme,
            metricsScheme,
            historyScheme,
            directory,
            true);

        auto response = uploader->UploadStats(
            { BuildTestStats() },
            { BuildTestMetrics() }).GetValueSync();
        UNIT_ASSERT(
            response.GetCode() == S_OK &&
            ydbTestStorage->AlterTableCalls == 2);
    }

    Y_UNIT_TEST(ShouldFailIfAlterChangesTypeOfExistingColumns)
    {
        auto config = CreateTestConfig();
        auto statsScheme = CreateStatsTestScheme();
        auto historyScheme = CreateHistoryTestScheme();
        auto statsNewScheme = CreateBadStatsTestScheme();
        auto historyNewScheme = CreateBadHistoryTestScheme();
        auto archiveNewScheme = CreateBadArchiveStatsTestScheme();
        auto metricsScheme = CreateMetricsTestScheme();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            statsNewScheme,
            historyNewScheme,
            archiveNewScheme,
            metricsScheme);
        uploader->Start();

        TVector<TTableStat> directory;
        directory.push_back(std::make_pair(TString("test"), TInstant::Now() - TDuration::Days(5)));
        ydbTestStorage->AddTables(
            statsScheme,
            metricsScheme,
            historyScheme,
            directory,
            true);

        auto response = uploader->UploadStats(
            { BuildTestStats() },
            { BuildTestMetrics() }).GetValueSync();
        UNIT_ASSERT(
            response.GetCode() == E_ARGUMENT &&
            ydbTestStorage->AlterTableCalls == 0);
    }

    Y_UNIT_TEST(ShouldDeleteExpiredHistoryTables)
    {
        auto config = CreateTestConfig();
        auto statsScheme = CreateStatsTestScheme();
        auto historyScheme = CreateHistoryTestScheme();
        auto archiveScheme = CreateArchiveStatsTestScheme();
        auto metricsScheme = CreateMetricsTestScheme();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            statsScheme,
            historyScheme,
            archiveScheme,
            metricsScheme);
        uploader->Start();

        TVector<TTableStat> directory;
        directory.push_back(std::make_pair(TString("test"), TInstant::Now() - TDuration::Days(5)));
        ydbTestStorage->AddTables(
            statsScheme,
            metricsScheme,
            historyScheme,
            directory,
            true);

        auto response = uploader->UploadStats(
            { BuildTestStats() },
            { BuildTestMetrics() }).GetValueSync();
        UNIT_ASSERT(
            response.GetCode() == S_OK &&
            ydbTestStorage->DropTableCalls == 1);
    }

    Y_UNIT_TEST(ShouldUploadData)
    {
        THashSet<TString> tables;

        auto config = CreateAllTablesTestConfig();
        auto statsScheme = CreateStatsTestScheme();
        auto historyScheme = CreateHistoryTestScheme();
        auto archiveScheme = CreateArchiveStatsTestScheme();
        auto metricsScheme = CreateMetricsTestScheme();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            statsScheme,
            historyScheme,
            archiveScheme,
            metricsScheme);
        uploader->Start();

        ydbTestStorage->UploadTestFunc = [&] (TString tableName, NYdb::TValue)
        {
            UNIT_ASSERT_C(
                !tables.contains(tableName),
                TStringBuilder() << "Table already seen " << tableName);
            tables.insert(tableName);

            return MakeFuture(MakeError(S_OK));
        };

        auto response = uploader->UploadStats(
             { BuildTestStats() },
             { BuildTestMetrics() }).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(4, ydbTestStorage->UpsertCalls);

        for (const auto& t: tables) {
            UNIT_ASSERT_C(
                t.Contains(config->GetDatabaseName()),
                TStringBuilder() <<
                    "table path " <<
                    t <<
                    " does not contain database name");
        }
    }

    Y_UNIT_TEST(ShouldReportNotFoundInCaseOfSchemeErrors)
    {
        auto config = CreateAllTablesTestConfig();
        auto statsScheme = CreateStatsTestScheme();
        auto historyScheme = CreateHistoryTestScheme();
        auto archiveScheme = CreateArchiveStatsTestScheme();
        auto metricsScheme = CreateMetricsTestScheme();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            statsScheme,
            historyScheme,
            archiveScheme,
            metricsScheme);
        uploader->Start();

        ydbTestStorage->UploadTestFunc = [&] (TString, NYdb::TValue)
        {
            if (ydbTestStorage->UpsertCalls == 1) {
                return MakeFuture(MakeError(S_OK));
            }
            if (ydbTestStorage->UpsertCalls == 2) {
                return MakeFuture(MakeError(E_NOT_FOUND));
            }
            return MakeFuture(MakeError(E_FAIL));
        };

        auto response = uploader->UploadStats(
             { BuildTestStats() },
             { BuildTestMetrics() }).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(4, ydbTestStorage->UpsertCalls);
    }

    Y_UNIT_TEST(ShouldCreateTablesWithTtlSessingsIfNessesary)
    {
        auto statsTtl = TDuration::Seconds(1);
        auto archiveStatsTtl = TDuration::Seconds(2);
        auto config = CreateTestConfig(
            statsTtl,
            archiveStatsTtl);
        auto statsScheme = CreateStatsTestScheme(statsTtl);
        auto historyScheme = CreateHistoryTestScheme();
        auto archiveScheme = CreateArchiveStatsTestScheme(archiveStatsTtl);
        auto metricsScheme = CreateMetricsTestScheme();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            statsScheme,
            historyScheme,
            archiveScheme,
            metricsScheme);
        uploader->Start();

        auto response = uploader->UploadStats(
             { BuildTestStats() },
             { BuildTestMetrics() }).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(4, ydbTestStorage->CreateTableCalls);

        {
            auto response = ydbTestStorage->DescribeTable("test").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(
                "DiskId",
                response.TableScheme.Ttl->GetValueSinceUnixEpoch().GetColumnName());
            UNIT_ASSERT_VALUES_EQUAL(
                NTable::TTtlSettings::EUnit::MilliSeconds,
                response.TableScheme.Ttl->GetValueSinceUnixEpoch().GetColumnUnit());
            UNIT_ASSERT_VALUES_EQUAL(
                statsTtl,
                response.TableScheme.Ttl->GetValueSinceUnixEpoch().GetExpireAfter());
        }

        {
            auto response = ydbTestStorage->DescribeTable("arctest").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(
                "DiskId",
                response.TableScheme.Ttl->GetValueSinceUnixEpoch().GetColumnName());
            UNIT_ASSERT_VALUES_EQUAL(
                NTable::TTtlSettings::EUnit::MilliSeconds,
                response.TableScheme.Ttl->GetValueSinceUnixEpoch().GetColumnUnit());
            UNIT_ASSERT_VALUES_EQUAL(
                archiveStatsTtl,
                response.TableScheme.Ttl->GetValueSinceUnixEpoch().GetExpireAfter());
        }
    }

    Y_UNIT_TEST(ShouldSetProperTtlSettingDuringAlter)
    {
        auto statsTtl = TDuration::Seconds(1);
        auto archiveStatsTtl = TDuration::Seconds(2);
        auto config = CreateTestConfig(
            statsTtl,
            archiveStatsTtl);
        auto statsScheme = CreateStatsTestScheme(statsTtl);
        auto historyScheme = CreateHistoryTestScheme();
        auto statsNewScheme = CreateStatsTestScheme();
        auto historyNewScheme = CreateNewHistoryTestScheme();
        auto archiveScheme = CreateNewArchiveStatsTestScheme(statsTtl);
        auto archiveNewScheme = CreateNewArchiveStatsTestScheme(archiveStatsTtl);
        auto metricsScheme = CreateMetricsTestScheme();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            statsNewScheme,
            historyNewScheme,
            archiveNewScheme,
            metricsScheme);
        uploader->Start();

        TVector<TTableStat> directory;
        directory.push_back(std::make_pair(TString("test"), TInstant::Now()));
        ydbTestStorage->AddTables(
            statsScheme,
            metricsScheme,
            historyScheme,
            directory,
            true);
        ydbTestStorage->AddTable("arctest", archiveScheme);

        auto response = uploader->UploadStats(
            { BuildTestStats() },
            { BuildTestMetrics() }).GetValueSync();
        UNIT_ASSERT(
            response.GetCode() == S_OK &&
            ydbTestStorage->AlterTableCalls == 1);

        {
            auto response = ydbTestStorage->DescribeTable("test").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(
                "DiskId",
                response.TableScheme.Ttl->GetValueSinceUnixEpoch().GetColumnName());
            UNIT_ASSERT_VALUES_EQUAL(
                NTable::TTtlSettings::EUnit::MilliSeconds,
                response.TableScheme.Ttl->GetValueSinceUnixEpoch().GetColumnUnit());
            UNIT_ASSERT_VALUES_EQUAL(
                statsTtl,
                response.TableScheme.Ttl->GetValueSinceUnixEpoch().GetExpireAfter());
        }

        {
            auto response = ydbTestStorage->DescribeTable("arctest").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());

            UNIT_ASSERT_VALUES_EQUAL(
                "DiskId",
                response.TableScheme.Ttl->GetValueSinceUnixEpoch().GetColumnName());
            UNIT_ASSERT_VALUES_EQUAL(
                NTable::TTtlSettings::EUnit::MilliSeconds,
                response.TableScheme.Ttl->GetValueSinceUnixEpoch().GetColumnUnit());
            UNIT_ASSERT_VALUES_EQUAL(
                statsTtl,
                response.TableScheme.Ttl->GetValueSinceUnixEpoch().GetExpireAfter());
        }
    }
}

}   // namespace NCloud::NBlockStore::NYdbStats
