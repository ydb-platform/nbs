#include "ydbstats.h"

#include "config.h"
#include "ydbrow.h"
#include "ydbscheme.h"
#include "ydbstorage.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/mutex.h>

#include <optional>

namespace NCloud::NBlockStore::NYdbStats {

using namespace NThreading;

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TAlterCheckResult
{
    const NProto::TError Error;
    const THashMap<TString, EPrimitiveType> Columns;
    const TMaybe<NYdb::NTable::TTtlSettings> Ttl;

    TAlterCheckResult(NProto::TError error)
        : Error(std::move(error))
    {}

    TAlterCheckResult(
            THashMap<TString, EPrimitiveType> columns,
            TMaybe<NYdb::NTable::TTtlSettings> ttl)
        : Columns(std::move(columns))
        , Ttl(std::move(ttl))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TSetupTableResult
{
    const NProto::TError Error;
    const TString TableName;

    TSetupTableResult(NProto::TError error, TString tableName = {})
        : Error(std::move(error))
        , TableName(std::move(tableName))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TYDBTableNames
{
    TString Stats;
    TString History;
    TString Archive;
    TString Metrics;
    TString Groups;
    TString Partitions;

    TYDBTableNames() = default;
};

struct TSetupTablesResult
{
    const NProto::TError Error;
    const TYDBTableNames TableNames;

    TSetupTablesResult(
            NProto::TError error,
            TYDBTableNames tableNames)
        : Error(std::move(error))
        , TableNames(std::move(tableNames))
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TWaitSetup
{
private:
    TAdaptiveLock Lock;
    size_t ResponsesToWait = 6;

    NProto::TError Error;
    TYDBTableNames TableNames;

    TPromise<TSetupTablesResult> Result = NewPromise<TSetupTablesResult>();

public:
    void SetStatsTable(const TSetupTableResult& result)
    {
        with_lock (Lock) {
            TableNames.Stats = result.TableName;
            CheckForCompletion(result.Error);
        }
    }

    void SetHistoryTable(const TSetupTableResult& result)
    {
        with_lock (Lock) {
            TableNames.History = result.TableName;
            CheckForCompletion(result.Error);
        }
    }

    void SetArchiveStatsTable(const TSetupTableResult& result)
    {
        with_lock (Lock) {
            TableNames.Archive = result.TableName;
            CheckForCompletion(result.Error);
        }
    }

    void SetMetricsTable(const TSetupTableResult& result)
    {
        with_lock (Lock) {
            TableNames.Metrics = result.TableName;
            CheckForCompletion(result.Error);
        }
    }

    void SetGroupsTable(const TSetupTableResult& result)
    {
        with_lock (Lock) {
            TableNames.Groups = result.TableName;
            CheckForCompletion(result.Error);
        }
    }

    void SetPartitionsTable(const TSetupTableResult& result)
    {
        with_lock (Lock) {
            TableNames.Partitions = result.TableName;
            CheckForCompletion(result.Error);
        }
    }

    TFuture<TSetupTablesResult> GetResult() const
    {
        return Result;
    }

private:
    void CheckForCompletion(const NProto::TError& error)
    {
        if (FAILED(error.GetCode())) {
            Error = error;
        }

        STORAGE_VERIFY_C(
            ResponsesToWait,
            TWellKnownEntityTypes::YDB_TABLE,
            "TWaitSetup",
            " too many responses while waiting for tables setup");
        if (--ResponsesToWait == 0) {
            Result.SetValue(
                TSetupTablesResult(
                    Error,
                    TableNames));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWaitRemove
{
private:
    TAdaptiveLock Lock;
    size_t ResponsesToWait;

    NProto::TError Error;

    TPromise<NProto::TError> Result = NewPromise<NProto::TError>();

public:
    TWaitRemove(size_t responsesToWait)
        : ResponsesToWait(responsesToWait)
    {}

    void SetResult(const NProto::TError& error)
    {
        with_lock (Lock) {
            CheckForCompletion(error);
        }
    }

    TFuture<NProto::TError> GetResult() const
    {
        return Result;
    }

private:
    void CheckForCompletion(const NProto::TError& error)
    {
        if (FAILED(error.GetCode())) {
            Error = error;
        }

        STORAGE_VERIFY_C(
            ResponsesToWait,
            TWellKnownEntityTypes::YDB_TABLE,
            "TWaitRemove",
            " too many responses while waiting for tables drop");
        if (--ResponsesToWait == 0) {
            Result.SetValue(Error);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TUploadData
{
    TString TableName;
    NYdb::TValue UploadData;

    TUploadData(TString tableName, NYdb::TValue uploadData)
        : TableName(std::move(tableName))
        , UploadData(std::move(uploadData))
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TBulkUpsertExecutor
    : public std::enable_shared_from_this<TBulkUpsertExecutor>
{
    const IYdbStoragePtr Storage;
    const TString DatabaseName;
    TVector<TUploadData> DataToUpload;
    TVector<TFuture<NProto::TError>> Responses;

    TAdaptiveLock ResponseLock;
    TPromise<NProto::TError> Response = NewPromise<NProto::TError>();
    ui32 ResponsesToWait = 0;

public:
    TBulkUpsertExecutor(
            IYdbStoragePtr storage,
            TString databaseName,
            TVector<TUploadData> dataToUpload)
        : Storage{std::move(storage)}
        , DatabaseName{std::move(databaseName)}
        , DataToUpload{std::move(dataToUpload)}
    {}

    TFuture<NProto::TError> Execute()
    {
        Responses.reserve(DataToUpload.size());
        ResponsesToWait = DataToUpload.size();
        for (auto& data: DataToUpload) {
            auto fullName = DatabaseName + '/' + data.TableName;
            Responses.emplace_back(Storage->ExecuteUploadQuery(
                std::move(fullName),
                std::move(data.UploadData)));

            Responses.back().Subscribe(
                [pThis = shared_from_this()] (const auto& future) mutable {
                    Y_UNUSED(future);
                    with_lock (pThis->ResponseLock) {
                        if (!--pThis->ResponsesToWait) {
                            pThis->SetResponse();
                        }
                    }
            });
        }
        return Response.GetFuture();
    }

private:
    void SetResponse()
    {
        NProto::TError result;

        for (const auto& f: Responses) {
            auto error = f.GetValue();
            if (FAILED(error.GetCode())) {
                if (SUCCEEDED(result.GetCode()) || error.GetCode() == E_NOT_FOUND) {
                    result = std::move(error);
                }
            }
        }
        Response.SetValue(std::move(result));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYdbStatsUploader final
    : public IYdbVolumesStatsUploader
    , public std::enable_shared_from_this<TYdbStatsUploader>
{
    enum EState
    {
        UNINITIALIZED,
        INITIALIZING,
        WORKING
    };

private:
    const TYdbStatsConfigPtr Config;
    const ILoggingServicePtr Logging;

    const IYdbStoragePtr DbStorage;
    const TYDBTableSchemes TableSchemes;

    TMutex StateLock;
    EState State = EState::UNINITIALIZED;
    TPromise<TSetupTablesResult> InitResponse;

    TInstant HistoryTableTimestamp;

    bool CleanupRunning = false;
    TInstant LastCleanupCheck;

    TLog Log;

public:
    TYdbStatsUploader(
        TYdbStatsConfigPtr config,
        ILoggingServicePtr logging,
        IYdbStoragePtr dbStorage,
        TYDBTableSchemes tableSchemes);

    TFuture<NProto::TError> UploadStats(const TYdbRowData& rows) override;

    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_YDBSTATS");
    }

    void Stop() override
    {
    }

private:
    TFuture<NProto::TError> DoUploadStats(
        const TYdbRowData& rows,
        const TSetupTablesResult& setupResult);

    TFuture<TSetupTablesResult> EnsureInitialized();

    void HandleSetupTablesResult(const TSetupTablesResult& setupResult);
    void HandleRemoveObsoleteTablesResult(const NProto::TError& error);

    TFuture<TSetupTablesResult> SetupTables() const;
    TFuture<TSetupTableResult> SetupStatsTable() const;
    TFuture<TSetupTableResult> SetupHistoryTable() const;
    TFuture<TSetupTableResult> SetupArchiveStatsTable() const;
    TFuture<TSetupTableResult> SetupMetricsTable() const;
    TFuture<TSetupTableResult> SetupGroupsTable() const;
    TFuture<TSetupTableResult> SetupPartitionsTable() const;

    TFuture<NProto::TError> SetupTable(
        const TString& tableName,
        TStatsTableSchemePtr tableScheme) const;

    TFuture<NProto::TError> CreateTable(
        const TString& tableName,
        const TStatsTableScheme& tableScheme) const;

    TFuture<NProto::TError> AlterTable(
        const TString& tableName,
        const TStatsTableScheme& tableScheme,
        const TStatsTableScheme& existingScheme) const;

    bool IsRotationRequired(TInstant ts) const;
    TString FormatHistoryTableName(TInstant ts) const;

    TFuture<NProto::TError> RemoveObsoleteTables() const;

    TFuture<NProto::TError> RemoveObsoleteTables(
        const TVector<TTableStat>& historyTables) const;

    TAlterCheckResult CheckForAlter(
        const TStatsTableScheme& existingColumns,
        const TStatsTableScheme& newColumns) const;
};

////////////////////////////////////////////////////////////////////////////////

TYdbStatsUploader::TYdbStatsUploader(
        TYdbStatsConfigPtr config,
        ILoggingServicePtr logging,
        IYdbStoragePtr dbStorage,
        TYDBTableSchemes tableSchemes)
    : Config(std::move(config))
    , Logging(std::move(logging))
    , DbStorage(std::move(dbStorage))
    , TableSchemes(std::move(tableSchemes))
{}

////////////////////////////////////////////////////////////////////////////////
// UploadStats

TFuture<NProto::TError> TYdbStatsUploader::UploadStats(const TYdbRowData& rows)
{
    auto response = EnsureInitialized();
    if (response.HasValue()) {
        return DoUploadStats(rows, response.GetValue());
    }

    auto pThis = shared_from_this();  // will hold reference to this
    return response.Apply(
        [=] (const auto& future) {
            return pThis->DoUploadStats(rows, future.GetValue());
        });
}

TFuture<NProto::TError> TYdbStatsUploader::DoUploadStats(
    const TYdbRowData& rows,
    const TSetupTablesResult& setupResult)
{
    if (FAILED(setupResult.Error.GetCode())) {
        return MakeFuture(setupResult.Error);
    }

    TVector<TUploadData> dataForUpload;

    auto addStatsRows = [&] (TValueBuilder& rowsBuilder) {
        for (const auto& row: rows.Stats) {
            rowsBuilder.AddListItem(row.GetYdbValues());
        }
    };
    auto addMetricsRows = [&] (TValueBuilder& rowsBuilder) {
        for (const auto& row : rows.Metrics) {
            rowsBuilder.AddListItem(row.GetYdbValues());
        }
    };
    auto addGroupsRows = [&](TValueBuilder& rowsBuilder)
    {
        for (const auto& row: rows.Groups) {
            rowsBuilder.AddListItem(row.GetYdbValues());
        }
    };
    auto addPartitionsRows = [&](TValueBuilder& rowsBuilder)
    {
        for (const auto& row: rows.Partitions) {
            rowsBuilder.AddListItem(row.GetYdbValues());
        }
    };

    auto buildYdbRows = [&] (const std::function<void(TValueBuilder&)>& addRows) {
        TValueBuilder rowsBuilder;

        rowsBuilder.BeginList();
        addRows(rowsBuilder);
        rowsBuilder.EndList();

        return rowsBuilder.Build();
    };

    if (setupResult.TableNames.Stats) {
        // TODO: avoid explicitly making copies
        // of stats after https://github.com/ydb-platform/ydb/issues/1659
        dataForUpload.emplace_back(
            setupResult.TableNames.Stats,
            buildYdbRows(addStatsRows));
    }

    if (setupResult.TableNames.History) {
        // TODO: avoid explicitly making copies
        // of stats after https://github.com/ydb-platform/ydb/issues/1659
        dataForUpload.emplace_back(
            setupResult.TableNames.History,
            buildYdbRows(addStatsRows));
    }

    if (setupResult.TableNames.Archive) {
        // TODO: avoid explicitly making copies
        // of stats after https://github.com/ydb-platform/ydb/issues/1659
        dataForUpload.emplace_back(
            setupResult.TableNames.Archive,
            buildYdbRows(addStatsRows));
    }

    if (setupResult.TableNames.Metrics) {
        dataForUpload.emplace_back(
            setupResult.TableNames.Metrics,
            buildYdbRows(addMetricsRows));
    }

    if (setupResult.TableNames.Groups) {
        dataForUpload.emplace_back(
            setupResult.TableNames.Groups,
            buildYdbRows(addGroupsRows));
    }

    if (setupResult.TableNames.Partitions) {
        dataForUpload.emplace_back(
            setupResult.TableNames.Partitions,
            buildYdbRows(addPartitionsRows));
    }

    auto executor = std::make_shared<TBulkUpsertExecutor>(
        DbStorage,
        Config->GetDatabaseName(),
        std::move(dataForUpload));
    auto future = executor->Execute();

    auto pThis = shared_from_this();  // will hold reference to this
    return future.Apply(
        [pThis] (const auto& future) {
            const auto& result = future.GetValue();
            if (result.GetCode() == E_NOT_FOUND) {
                with_lock (pThis->StateLock) {
                    pThis->State = EState::UNINITIALIZED;
                }
            }
            return future;
        });
}

////////////////////////////////////////////////////////////////////////////////
// EnsureInitialized

TFuture<TSetupTablesResult> TYdbStatsUploader::EnsureInitialized()
{
    with_lock (StateLock) {
        if (State == EState::UNINITIALIZED ||
           (State == EState::WORKING && IsRotationRequired(HistoryTableTimestamp)))
        {
            InitResponse = NewPromise<TSetupTablesResult>();
            State = EState::INITIALIZING;
            SetupTables().Subscribe(
                [=, this] (const auto& future) {
                    HandleSetupTablesResult(future.GetValue());
                });
        }

        if (State == WORKING) {
            auto sinceLastCheck = (TInstant::Now() - LastCleanupCheck).Days();
            auto threshold = Config->GetHistoryTableLifetimeDays();
            if (!CleanupRunning && sinceLastCheck >= threshold) {
                CleanupRunning = true;
                STORAGE_INFO("Going to check for obsolete history tables");
                RemoveObsoleteTables().Subscribe(
                    [=, this] (const auto& future) {
                        HandleRemoveObsoleteTablesResult(future.GetValue());
                    });
            }
        }

        return InitResponse;
    }
}

void TYdbStatsUploader::HandleSetupTablesResult(
    const TSetupTablesResult& setupResult)
{
    TPromise<TSetupTablesResult> response;
    with_lock (StateLock) {
        if (SUCCEEDED(setupResult.Error.GetCode())) {
            if (State == EState::INITIALIZING) {
                State = EState::WORKING;
                HistoryTableTimestamp = TInstant::Now();
            }
        } else {
            State = EState::UNINITIALIZED;
        }
        response = InitResponse;
    }

    // should be called outside of the lock
    response.SetValue(setupResult);
}

void TYdbStatsUploader::HandleRemoveObsoleteTablesResult(
    const NProto::TError& error)
{
    with_lock (StateLock) {
        if (SUCCEEDED(error.GetCode())) {
            LastCleanupCheck = TInstant::Now();
        } else {
            auto text = TStringBuilder()
                << "Unable to remove obsolete history tables, "
                << " error code: " << error.GetCode()
                << " with reason:\n" << error.GetMessage();
            STORAGE_ERROR(text);
        }
        CleanupRunning = false;
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetupTables

TFuture<TSetupTablesResult> TYdbStatsUploader::SetupTables() const
{
    auto pThis = shared_from_this();  // will hold reference to this

    auto waitSetup = std::make_shared<TWaitSetup>();

    SetupStatsTable().Subscribe(
        [pThis, waitSetup] (const auto& future) {
            waitSetup->SetStatsTable(future.GetValue());
        });

    SetupHistoryTable().Subscribe(
        [pThis, waitSetup] (const auto& future) {
            waitSetup->SetHistoryTable(future.GetValue());
        });

    SetupArchiveStatsTable().Subscribe(
        [pThis, waitSetup] (const auto& future) {
            waitSetup->SetArchiveStatsTable(future.GetValue());
        });

    SetupMetricsTable().Subscribe(
        [pThis, waitSetup] (const auto& future) {
            waitSetup->SetMetricsTable(future.GetValue());
        });

    SetupGroupsTable().Subscribe(
        [pThis, waitSetup](const auto& future) {
            waitSetup->SetGroupsTable(future.GetValue());
        });

    SetupPartitionsTable().Subscribe(
        [pThis, waitSetup](const auto& future) {
            waitSetup->SetPartitionsTable(future.GetValue());
        });

    return waitSetup->GetResult();
}

TFuture<TSetupTableResult> TYdbStatsUploader::SetupStatsTable() const
{
    auto tableName = Config->GetStatsTableName();
    return SetupTable(tableName, TableSchemes.Stats).Apply(
        [=] (const auto& future) {
            return TSetupTableResult(future.GetValue(), tableName);
        });
}

TFuture<TSetupTableResult> TYdbStatsUploader::SetupArchiveStatsTable() const
{
    auto tableName = Config->GetArchiveStatsTableName();
    if (!tableName) {
        return MakeFuture(TSetupTableResult(MakeError(S_OK), tableName));
    }
    return SetupTable(tableName, TableSchemes.Archive).Apply(
        [=] (const auto& future) {
            return TSetupTableResult(future.GetValue(), tableName);
        });
}

TFuture<TSetupTableResult> TYdbStatsUploader::SetupMetricsTable() const
{
    auto tableName = Config->GetBlobLoadMetricsTableName();
    if (!tableName) {
        return MakeFuture(TSetupTableResult(MakeError(S_OK), tableName));
    }
    return SetupTable(tableName, TableSchemes.Metrics).Apply(
        [=] (const auto& future) {
            return TSetupTableResult(future.GetValue(), tableName);
        });
}

TFuture<TSetupTableResult> TYdbStatsUploader::SetupGroupsTable() const
{
    auto tableName = Config->GetGroupsTableName();
    if (!tableName) {
        return MakeFuture(TSetupTableResult(MakeError(S_OK), tableName));
    }
    return SetupTable(tableName, TableSchemes.Groups)
        .Apply([=](const auto& future)
               { return TSetupTableResult(future.GetValue(), tableName); });
}

TFuture<TSetupTableResult> TYdbStatsUploader::SetupPartitionsTable() const
{
    auto tableName = Config->GetPartitionsTableName();
    if (!tableName) {
        return MakeFuture(TSetupTableResult(MakeError(S_OK), tableName));
    }
    return SetupTable(tableName, TableSchemes.Partitions)
        .Apply([=](const auto& future)
               { return TSetupTableResult(future.GetValue(), tableName); });
}

TFuture<TSetupTableResult> TYdbStatsUploader::SetupHistoryTable() const
{
    return DbStorage->GetHistoryTables().Apply(
        [=, this] (const auto& future) {
            const auto& result = future.GetValue();
            if (FAILED(result.Error.GetCode())) {
                return MakeFuture(TSetupTableResult(result.Error));
            }

            TString tableName;
            TInstant creationTime;
            for (const auto& i: result.Tables) {
                if (!i.first.StartsWith(Config->GetHistoryTablePrefix())) {
                    continue;
                }
                if (creationTime < i.second) {
                    tableName = i.first;
                    creationTime = i.second;
                }
            }

            if (!tableName || IsRotationRequired(creationTime)) {
                tableName = FormatHistoryTableName(TInstant::Now());
            }

            return SetupTable(tableName, TableSchemes.History).Apply(
                [=] (const auto& future) {
                    return TSetupTableResult(future.GetValue(), tableName);
                });
        });
}

TFuture<NProto::TError> TYdbStatsUploader::SetupTable(
    const TString& tableName,
    TStatsTableSchemePtr tableScheme) const
{
    return DbStorage->DescribeTable(tableName).Apply(
        [=, this] (const auto& future) {
            const auto& response = future.GetValue();
            if (SUCCEEDED(response.Error.GetCode())) {
                return AlterTable(tableName, *tableScheme, response.TableScheme);
            } else if (response.Error.GetCode() == E_NOT_FOUND) {
                return CreateTable(tableName, *tableScheme);
            } else {
                return MakeFuture(response.Error);
            }
        });
}

TFuture<NProto::TError> TYdbStatsUploader::CreateTable(
    const TString& tableName,
    const TStatsTableScheme& tableScheme) const
{
    TTableBuilder tableBuilder;
    for (const auto& column: tableScheme.Columns) {
        tableBuilder.AddNullableColumn(
            column.Name,
            TTypeParser(column.Type).GetPrimitive());
    }

    tableBuilder.SetPrimaryKeyColumns(tableScheme.KeyColumns);

    if (tableScheme.Ttl) {
        tableBuilder.SetTtlSettings(*tableScheme.Ttl);
    }

    return DbStorage->CreateTable(tableName, tableBuilder.Build());
}

TFuture<NProto::TError> TYdbStatsUploader::AlterTable(
    const TString& tableName,
    const TStatsTableScheme& scheme,
    const TStatsTableScheme& existingScheme) const
{
    auto diff = CheckForAlter(existingScheme, scheme);
    if (FAILED(diff.Error.GetCode())) {
        return MakeFuture(diff.Error);
    }

    auto areTtlSettingsDifferent = [] (auto&& ttl1, auto&& ttl2) {
        return ttl1.GetColumnName() != ttl2.GetColumnName()
            || ttl1.GetColumnUnit() != ttl2.GetColumnUnit()
            || ttl1.GetExpireAfter() != ttl2.GetExpireAfter();
    };

    auto areTtlSettingsUpdated = [&] () {
        return scheme.Ttl.Empty()
            || diff.Ttl->GetMode() != scheme.Ttl->GetMode()
            || areTtlSettingsDifferent(
                diff.Ttl->GetValueSinceUnixEpoch(),
                scheme.Ttl->GetValueSinceUnixEpoch());
    };

    bool shouldUpdateTtl = diff.Ttl.Defined() && areTtlSettingsUpdated();

    if (!diff.Columns && !shouldUpdateTtl) {
        // nothing to do
        return MakeFuture<NProto::TError>();
    }

    TAlterTableSettings settings;
    for (const auto& c: diff.Columns) {
        TTypeBuilder builder;
        builder.BeginOptional();
        builder.Primitive(c.second);
        builder.EndOptional();

        settings.AppendAddColumns(TColumn(c.first, builder.Build()));
    }

    if (diff.Ttl.Defined()) {
        const auto& ttl = diff.Ttl->GetValueSinceUnixEpoch();
        settings.AlterTtlSettings(
            TAlterTtlSettings::Set(
                ttl.GetColumnName(),
                ttl.GetColumnUnit(),
                ttl.GetExpireAfter()));
    }

    return DbStorage->AlterTable(tableName, settings);
}

bool TYdbStatsUploader::IsRotationRequired(TInstant ts) const
{
    auto configValue = Config->GetStatsTableRotationAfterDays();
    auto currentDay = TInstant::Now().Days();
    return currentDay - ts.Days() >= configValue;
}

TString TYdbStatsUploader::FormatHistoryTableName(TInstant ts) const
{
    return TStringBuilder()
        << Config->GetHistoryTablePrefix()
        << '-'
        << ts.FormatLocalTime("%F");
}

////////////////////////////////////////////////////////////////////////////////
// RemoveObsoleteTables

TFuture<NProto::TError> TYdbStatsUploader::RemoveObsoleteTables() const
{
    auto pThis = shared_from_this();  // will hold reference to this

    return DbStorage->GetHistoryTables().Apply(
        [=] (const auto& future) {
            const auto& response = future.GetValue();
            if (FAILED(response.Error.GetCode())) {
                return MakeFuture(response.Error);
            }

            return pThis->RemoveObsoleteTables(response.Tables);
        });
}

TFuture<NProto::TError> TYdbStatsUploader::RemoveObsoleteTables(
    const TVector<TTableStat>& historyTables) const
{
    auto now = TInstant::Now().Days();

    TVector<TString> obsoleteTables;
    TStringBuilder obsoleteStr;
    for (const auto& table: historyTables) {
        if (!table.first.StartsWith(Config->GetHistoryTablePrefix()) ||
            (now < table.second.Days()))
        {
            continue;
        }
        if (now - table.second.Days() >= Config->GetHistoryTableLifetimeDays()) {
            obsoleteTables.push_back(table.first);
            obsoleteStr << table.first.Quote() << '\n';
        }
    }

    STORAGE_INFO("Going to remove obsolete tables: " << obsoleteStr);

    if (!obsoleteTables) {
        // nothing to do
        return MakeFuture<NProto::TError>();
    }

    auto waitRemove = std::make_shared<TWaitRemove>(obsoleteTables.size());
    for (const auto& tableName: obsoleteTables) {
        DbStorage->DropTable(tableName).Subscribe(
            [=] (const auto& future) {
                waitRemove->SetResult(future.GetValue());
            });
    }

    return waitRemove->GetResult();
}

TAlterCheckResult TYdbStatsUploader::CheckForAlter(
    const TStatsTableScheme& existingTable,
    const TStatsTableScheme& newTable) const
{
    THashMap<TString, EPrimitiveType> columnsSet;
    THashMap<TString, EPrimitiveType> diff;

    for (const auto& column: existingTable.Columns) {
        auto typeParser = TTypeParser(column.Type);
        typeParser.OpenOptional();

        if (!columnsSet.emplace(column.Name, typeParser.GetPrimitive()).second) {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "duplicate field: " << column.Name);
        }

        typeParser.CloseOptional();
    }

    TStringBuilder newColumnList;

    for (const auto& column: newTable.Columns) {
        auto typeParser = TTypeParser(column.Type);

        auto it = columnsSet.find(column.Name);
        if (it == columnsSet.end()) {
            diff.emplace(column.Name, typeParser.GetPrimitive());
            newColumnList << column.Name << '\n';
        } else {
            if (typeParser.GetPrimitive() != it->second) {
                return MakeError(E_ARGUMENT, TStringBuilder()
                    << "type of field: " << column.Name << " has changed");
            }
        }
    }

    if (!diff.empty()) {
        STORAGE_INFO("New columns are: [" << newColumnList << ']')
    }

    return TAlterCheckResult{diff, newTable.Ttl};
}

////////////////////////////////////////////////////////////////////////////////

class TYdbStatsUploaderStub final
    : public IYdbVolumesStatsUploader
{
public:
    TFuture<NProto::TError> UploadStats(const TYdbRowData& rows) override
    {
        Y_UNUSED(rows);
        return MakeFuture<NProto::TError>();
    }

    void Start() override
    {
    }

    void Stop() override
    {
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TYDBTableSchemes::TYDBTableSchemes(
        TStatsTableSchemePtr stats,
        TStatsTableSchemePtr history,
        TStatsTableSchemePtr archive,
        TStatsTableSchemePtr metrics,
        TStatsTableSchemePtr groups,
        TStatsTableSchemePtr partitions)
    : Stats(std::move(stats))
    , History(std::move(history))
    , Archive(std::move(archive))
    , Metrics(std::move(metrics))
    , Groups(std::move(groups))
    , Partitions(std::move(partitions))
{}

TYDBTableSchemes::TYDBTableSchemes(TDuration statsTtl, TDuration archiveTtl)
    : Stats(CreateStatsTableScheme(statsTtl))
    , History(CreateHistoryTableScheme())
    , Archive(CreateArchiveStatsTableScheme(archiveTtl))
    , Metrics(CreateBlobLoadMetricsTableScheme())
    , Groups(CreateGroupsTableScheme())
    , Partitions(CreatePartitionsTableScheme())
{}

TYDBTableSchemes::~TYDBTableSchemes() = default;

IStartable* AsStartable(IYdbStoragePtr storagePtr)
{
    return storagePtr.get();
}

IYdbVolumesStatsUploaderPtr CreateYdbVolumesStatsUploader(
    TYdbStatsConfigPtr config,
    ILoggingServicePtr logging,
    IYdbStoragePtr dbStorage,
    TYDBTableSchemes tableSchemes)
{
    return std::make_unique<TYdbStatsUploader>(
        std::move(config),
        std::move(logging),
        std::move(dbStorage),
        std::move(tableSchemes));
}

IYdbVolumesStatsUploaderPtr CreateVolumesStatsUploaderStub()
{
    return std::make_unique<TYdbStatsUploaderStub>();
}

}   // namespace NCloud::NBlockStore::NYdbStats
