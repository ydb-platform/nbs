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
    config.SetGroupsTableName("groups");
    config.SetPartitionsTableName("partitions");

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
    config.SetGroupsTableName("groups");
    config.SetPartitionsTableName("partitions");
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
    config.SetGroupsTableName("groups");
    config.SetPartitionsTableName("partitions");
    config.SetDatabaseName("/Root");

    return std::make_shared<TYdbStatsConfig>(config);
}

////////////////////////////////////////////////////////////////////////////////

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

TStatsTableSchemePtr CreateNewArchiveStatsTestScheme(TDuration ttl)
{
    return CreateNewStatsTestScheme(ttl);
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

TStatsTableSchemePtr CreateGroupsTestScheme()
{
    TStatsTableSchemeBuilder out;
    static TVector<std::pair<TString, NYdb::EPrimitiveType>> columns = {
        {"TabletId", NYdb::EPrimitiveType::Uint64},
        {"GroudId", NYdb::EPrimitiveType::Uint32},
        {"Timestamp", NYdb::EPrimitiveType::Uint64},
    };
    out.SetKeyColumns({"TabletId", "GroudId"});
    out.AddColumns(columns);
    return out.Finish();
}

TStatsTableSchemePtr CreatePartitionsTestScheme()
{
    TStatsTableSchemeBuilder out;
    static TVector<std::pair<TString, NYdb::EPrimitiveType>> columns = {
        {"PartitionTabletId", NYdb::EPrimitiveType::Uint64},
        {"VolumeTabletId", NYdb::EPrimitiveType::Uint64},
        {"DiskId", NYdb::EPrimitiveType::String},
        {"Timestamp", NYdb::EPrimitiveType::Uint64},
    };
    out.SetKeyColumns({"PartitionTabletId"});
    out.AddColumns(columns);
    return out.Finish();
}

TYDBTableSchemes CreateTestSchemes(
    TDuration statesTtl = {},
    TDuration archiveTtl = {})
{
    return {
        CreateStatsTestScheme(statesTtl),
        CreateHistoryTestScheme(),
        CreateArchiveStatsTestScheme(archiveTtl),
        CreateMetricsTestScheme(),
        CreateGroupsTestScheme(),
        CreatePartitionsTestScheme()};
}

TYDBTableSchemes CreateNewTestSchemes(
    TDuration statesTtl = {},
    TDuration archiveTtl = {})
{
    return {
        CreateNewStatsTestScheme(statesTtl),
        CreateNewHistoryTestScheme(),
        CreateNewArchiveStatsTestScheme(archiveTtl),
        CreateMetricsTestScheme(),
        CreateGroupsTestScheme(),
        CreatePartitionsTestScheme()};
}

TYDBTableSchemes CreateBadTestSchemes()
{
    return {
        CreateBadStatsTestScheme(),
        CreateBadHistoryTestScheme(),
        CreateBadArchiveStatsTestScheme(),
        CreateMetricsTestScheme(),
        CreateGroupsTestScheme(),
        CreatePartitionsTestScheme()};
}

////////////////////////////////////////////////////////////////////////////////

NYdbStats::TYdbStatsRow BuildTestStats()
{
    TYdbStatsRow out;
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

NYdbStats::TYdbGroupsRow BuildTestGroups()
{
    TYdbGroupsRow out;
    out.TabletId = 713;
    out.GroupId = 42;
    out.Timestamp = TInstant::Now();
    return out;
}

NYdbStats::TYdbPartitionsRow BuildTestPartitions()
{
    TYdbPartitionsRow out;
    out.PartitionTabletId = 713;
    out.VolumeTabletId = 712;
    out.DiskId = "vol0";
    out.Timestamp = TInstant::Now();
    return out;
}

NYdbStats::TYdbRowData BuildTestYdbRowData()
{
    return {
        { BuildTestStats() },
        { BuildTestMetrics() },
        { BuildTestGroups() },
        { BuildTestPartitions() }};
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
        TYDBTableSchemes tableSchemes,
        const TVector<TTableStat>& historyTables,
        bool statTable)
    {
        for (const auto& t : historyTables) {
            Tables.emplace(t.first + "-" + t.second.FormatLocalTime("%F"), tableSchemes.History);
        }
        if (statTable) {
            Tables.emplace(Config->GetStatsTableName(), tableSchemes.Stats);
        }
        if (tableSchemes.Metrics) {
            Tables.emplace(Config->GetBlobLoadMetricsTableName(), tableSchemes.Metrics);
        }
        if (tableSchemes.Groups) {
            Tables.emplace(Config->GetGroupsTableName(), tableSchemes.Groups);
        }
        if (tableSchemes.Partitions) {
            Tables.emplace(
                Config->GetPartitionsTableName(),
                tableSchemes.Partitions);
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
        auto schemes = CreateTestSchemes();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            schemes);
        uploader->Start();

        auto response = uploader->UploadStats(BuildTestYdbRowData()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(5, ydbTestStorage->CreateTableCalls);
    }

    Y_UNIT_TEST(ShouldNotCreateTablesIfTheyAlreadyExistAndNotOutdated)
    {
        auto config = CreateTestConfig();
        auto schemes = CreateTestSchemes();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            schemes);
        uploader->Start();

        TVector<TTableStat> directory;
        directory.push_back(std::make_pair(TString("test"), TInstant::Now()));
        directory.push_back(std::make_pair(TString("test"), TInstant::Now() - TDuration::Days(5)));
        ydbTestStorage->AddTables(schemes, directory, true);

        auto response = uploader->UploadStats(BuildTestYdbRowData()).GetValueSync();
        UNIT_ASSERT(
            response.GetCode() == S_OK &&
            ydbTestStorage->CreateTableCalls == 0);
    }

    Y_UNIT_TEST(ShouldCreateNewHistoryTableIfItIsOutdated)
    {
        auto config = CreateTestConfig();
        auto schemes = CreateTestSchemes();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            schemes);
        uploader->Start();

        TVector<TTableStat> directory;
        directory.push_back(std::make_pair(TString("test"), TInstant::Now() - TDuration::Days(5)));
        ydbTestStorage->AddTables(
            schemes,
            directory,
            true);

        auto response = uploader->UploadStats(BuildTestYdbRowData()).GetValueSync();
        UNIT_ASSERT(
            response.GetCode() == S_OK &&
            ydbTestStorage->CreateTableCalls == 1);
    }

    Y_UNIT_TEST(ShouldAlterTables)
    {
        auto config = CreateTestConfig();
        auto schemes = CreateTestSchemes();
        auto newSchemes = CreateNewTestSchemes();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            newSchemes);
        uploader->Start();

        TVector<TTableStat> directory;
        directory.push_back(std::make_pair(TString("test"), TInstant::Now()));
        ydbTestStorage->AddTables(
            schemes,
            directory,
            true);

        auto response = uploader->UploadStats(BuildTestYdbRowData()).GetValueSync();
        UNIT_ASSERT(
            response.GetCode() == S_OK &&
            ydbTestStorage->AlterTableCalls == 2);
    }

    Y_UNIT_TEST(ShouldFailIfAlterChangesTypeOfExistingColumns)
    {
        auto config = CreateTestConfig();
        auto schemes = CreateTestSchemes();
        auto badSchemes = CreateBadTestSchemes();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            badSchemes);
        uploader->Start();

        TVector<TTableStat> directory;
        directory.push_back(std::make_pair(TString("test"), TInstant::Now() - TDuration::Days(5)));
        ydbTestStorage->AddTables(
            schemes,
            directory,
            true);

        auto response = uploader->UploadStats(BuildTestYdbRowData()).GetValueSync();
        UNIT_ASSERT(
            response.GetCode() == E_ARGUMENT &&
            ydbTestStorage->AlterTableCalls == 0);
    }

    Y_UNIT_TEST(ShouldDeleteExpiredHistoryTables)
    {
        auto config = CreateTestConfig();
        auto schemes = CreateTestSchemes();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            schemes);
        uploader->Start();

        TVector<TTableStat> directory;
        directory.push_back(std::make_pair(TString("test"), TInstant::Now() - TDuration::Days(5)));
        ydbTestStorage->AddTables(
            schemes,
            directory,
            true);

        auto response = uploader->UploadStats(BuildTestYdbRowData()).GetValueSync();
        UNIT_ASSERT(
            response.GetCode() == S_OK &&
            ydbTestStorage->DropTableCalls == 1);
    }

    Y_UNIT_TEST(ShouldUploadData)
    {
        THashSet<TString> tables;

        auto config = CreateAllTablesTestConfig();
        auto schemes = CreateTestSchemes();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            schemes);
        uploader->Start();

        ydbTestStorage->UploadTestFunc = [&] (TString tableName, NYdb::TValue)
        {
            UNIT_ASSERT_C(
                !tables.contains(tableName),
                TStringBuilder() << "Table already seen " << tableName);
            tables.insert(tableName);

            return MakeFuture(MakeError(S_OK));
        };

        auto response = uploader->UploadStats(BuildTestYdbRowData()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(6, ydbTestStorage->UpsertCalls);

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
        auto schemes = CreateTestSchemes();
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            schemes);
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

        auto response = uploader->UploadStats(BuildTestYdbRowData()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(6, ydbTestStorage->UpsertCalls);
    }

    Y_UNIT_TEST(ShouldCreateTablesWithTtlSessingsIfNessesary)
    {
        auto statsTtl = TDuration::Seconds(1);
        auto archiveStatsTtl = TDuration::Seconds(2);
        auto config = CreateTestConfig(
            statsTtl,
            archiveStatsTtl);
        auto schemes = CreateTestSchemes(statsTtl, archiveStatsTtl);
        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            schemes);
        uploader->Start();

        auto response = uploader->UploadStats(BuildTestYdbRowData()).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(6, ydbTestStorage->CreateTableCalls);

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
        auto groupsScheme = CreateGroupsTestScheme();
        auto partitionsScheme = CreatePartitionsTestScheme();

        auto ydbTestStorage = YdbCreateTestStorage(config);
        auto uploader = CreateYdbVolumesStatsUploader(
            config,
            CreateLoggingService("console"),
            ydbTestStorage,
            TYDBTableSchemes(
                statsNewScheme,
                historyNewScheme,
                archiveNewScheme,
                metricsScheme,
                groupsScheme,
                partitionsScheme));
        uploader->Start();

        TVector<TTableStat> directory;
        directory.push_back(std::make_pair(TString("test"), TInstant::Now()));
        ydbTestStorage->AddTables(
            TYDBTableSchemes(
                statsScheme,
                historyScheme,
                archiveScheme,
                metricsScheme,
                groupsScheme,
                partitionsScheme),
            directory,
            true);
        ydbTestStorage->AddTable("arctest", archiveScheme);

        auto response = uploader->UploadStats(BuildTestYdbRowData()).GetValueSync();
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
