#include "ydbstats.h"

#include "config.h"
#include "ydbrow.h"
#include "ydbscheme.h"
#include "ydbstorage.h"
#include "ydbwriters.h"

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/mutex.h>

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

    TAlterCheckResult(NProto::TError error)
        : Error(std::move(error))
    {}

    TAlterCheckResult(THashMap<TString, EPrimitiveType> columns)
        : Columns(std::move(columns))
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

struct TSetupTablesResult
{
    const NProto::TError Error;
    const TString StatsTableName;
    const TString HistoryTableName;
    const TString ArchiveStatsTableName;
    const TString BlobLoadMetricsTableName;

    TSetupTablesResult(
            NProto::TError error,
            TString statsTableName,
            TString historyTableName,
            TString archiveStatsTableName,
            TString blobLoadMetricsTableName)
        : Error(std::move(error))
        , StatsTableName(std::move(statsTableName))
        , HistoryTableName(std::move(historyTableName))
        , ArchiveStatsTableName(std::move(archiveStatsTableName))
        , BlobLoadMetricsTableName(std::move(blobLoadMetricsTableName))
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TWaitSetup
{
private:
    TAdaptiveLock Lock;
    size_t ResponsesToWait = 4;

    NProto::TError Error;
    TString StatsTableName;
    TString HistoryTableName;
    TString ArchiveStatsTableName;
    TString BlobLoadMetricsTableName;

    TPromise<TSetupTablesResult> Result = NewPromise<TSetupTablesResult>();

public:
    void SetStatsTable(const TSetupTableResult& result)
    {
        with_lock (Lock) {
            StatsTableName = result.TableName;
            CheckForCompletion(result.Error);
        }
    }

    void SetHistoryTable(const TSetupTableResult& result)
    {
        with_lock (Lock) {
            HistoryTableName = result.TableName;
            CheckForCompletion(result.Error);
        }
    }

    void SetArchiveStatsTable(const TSetupTableResult& result)
    {
        with_lock (Lock) {
            ArchiveStatsTableName = result.TableName;
            CheckForCompletion(result.Error);
        }
    }

    void SetMetricsTable(const TSetupTableResult& result)
    {
        with_lock (Lock) {
            BlobLoadMetricsTableName = result.TableName;
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

        Y_VERIFY(ResponsesToWait);
        if (--ResponsesToWait == 0) {
            Result.SetValue(
                TSetupTablesResult(
                    Error,
                    StatsTableName,
                    HistoryTableName,
                    ArchiveStatsTableName,
                    BlobLoadMetricsTableName));
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

        Y_VERIFY(ResponsesToWait);
        if (--ResponsesToWait == 0) {
            Result.SetValue(Error);
        }
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
    const TStatsTableSchemePtr StatsTableScheme;
    const TStatsTableSchemePtr HistoryTableScheme;
    const TStatsTableSchemePtr ArchiveStatsTableScheme;
    const TStatsTableSchemePtr MetricsTableScheme;

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
        TStatsTableSchemePtr statsTableScheme,
        TStatsTableSchemePtr historyTableScheme,
        TStatsTableSchemePtr archiveTableScheme,
        TStatsTableSchemePtr metricsTableScheme);

    TFuture<NProto::TError> UploadStats(
        const TVector<TYdbRow>& stats,
        const TVector<TYdbBlobLoadMetricRow>& metrics) override;

    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_YDBSTATS");
    }

    void Stop() override
    {
    }

private:
    TFuture<NProto::TError> DoUploadStats(
        const TVector<TYdbRow>& stats,
        const TVector<TYdbBlobLoadMetricRow>& metrics,
        const TSetupTablesResult& setupResult);

    TFuture<TSetupTablesResult> EnsureInitialized();

    void HandleSetupTablesResult(const TSetupTablesResult& setupResult);
    void HandleRemoveObsoleteTablesResult(const NProto::TError& error);

    TFuture<TSetupTablesResult> SetupTables() const;
    TFuture<TSetupTableResult> SetupStatsTable() const;
    TFuture<TSetupTableResult> SetupHistoryTable() const;
    TFuture<TSetupTableResult> SetupArchiveStatsTable() const;
    TFuture<TSetupTableResult> SetupMetricsTable() const;

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
        const TVector<NYdb::TColumn>& existingColumns,
        const TVector<NYdb::TColumn>& newColumns) const;
};

////////////////////////////////////////////////////////////////////////////////

TYdbStatsUploader::TYdbStatsUploader(
        TYdbStatsConfigPtr config,
        ILoggingServicePtr logging,
        IYdbStoragePtr dbStorage,
        TStatsTableSchemePtr statsTableScheme,
        TStatsTableSchemePtr historyTableScheme,
        TStatsTableSchemePtr archiveStatsTableScheme,
        TStatsTableSchemePtr metricsTableScheme)
    : Config(std::move(config))
    , Logging(std::move(logging))
    , DbStorage(std::move(dbStorage))
    , StatsTableScheme(std::move(statsTableScheme))
    , HistoryTableScheme(std::move(historyTableScheme))
    , ArchiveStatsTableScheme(std::move(archiveStatsTableScheme))
    , MetricsTableScheme(std::move(metricsTableScheme))
{}

////////////////////////////////////////////////////////////////////////////////
// UploadStats

TFuture<NProto::TError> TYdbStatsUploader::UploadStats(
    const TVector<TYdbRow>& stats,
    const TVector<TYdbBlobLoadMetricRow>& metrics)
{
    auto response = EnsureInitialized();
    if (response.HasValue()) {
        return DoUploadStats(stats, metrics, response.GetValue());
    }

    auto pThis = shared_from_this();  // will hold reference to this
    return response.Apply(
        [=] (const auto& future) {
            return pThis->DoUploadStats(stats, metrics, future.GetValue());
        });
}

TFuture<NProto::TError> TYdbStatsUploader::DoUploadStats(
    const TVector<TYdbRow>& stats,
    const TVector<TYdbBlobLoadMetricRow>& metrics,
    const TSetupTablesResult& setupResult)
{
    if (FAILED(setupResult.Error.GetCode())) {
        return MakeFuture(setupResult.Error);
    }

    TVector<const IYdbWriter*> dataForUpload;

    TYdbRowWriter statsTable(stats, setupResult.StatsTableName);
    TYdbReplaceWriter historyTable(
        setupResult.HistoryTableName,
        TYdbRowWriter::ItemName);
    TYdbReplaceWriter archiveTable(
        setupResult.ArchiveStatsTableName,
        TYdbRowWriter::ItemName);

    if (statsTable.IsValid()) {
        dataForUpload.emplace_back(&statsTable);
        if (historyTable.IsValid()) {
            dataForUpload.emplace_back(&historyTable);
        }
        if (archiveTable.IsValid()) {
            dataForUpload.emplace_back(&archiveTable);
        }
    }

    TYdbBlobLoadMetricWriter blobTable(
        metrics,
        setupResult.BlobLoadMetricsTableName);

    if (blobTable.IsValid()) {
        dataForUpload.emplace_back(&blobTable);
    }

    TStringStream out;
    out << "--!syntax_v1" << Endl
        << "PRAGMA TablePathPrefix(\""
        << Config->GetDatabaseName()
        << "\");"
        << Endl;

    for (const auto& element: dataForUpload) {
        element->Declare(out);
    }

    for (const auto& element: dataForUpload) {
        element->Replace(out);
    }

    TParamsBuilder paramsBuilder;
    for (const auto& element: dataForUpload) {
        element->PushData(paramsBuilder);
    }

    auto params = paramsBuilder.Build();
    auto pThis = shared_from_this();  // will hold reference to this

    STORAGE_DEBUG("Executing upload query:\n" << out.Str());

    auto future = DbStorage->ExecuteUploadQuery(out.Str(), std::move(params));

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
                [=] (const auto& future) {
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
                    [=] (const auto& future) {
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

    return waitSetup->GetResult();
}

TFuture<TSetupTableResult> TYdbStatsUploader::SetupStatsTable() const
{
    auto tableName = Config->GetStatsTableName();
    return SetupTable(tableName, StatsTableScheme).Apply(
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
    return SetupTable(tableName, ArchiveStatsTableScheme).Apply(
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
    return SetupTable(tableName, MetricsTableScheme).Apply(
        [=] (const auto& future) {
            return TSetupTableResult(future.GetValue(), tableName);
        });
}

TFuture<TSetupTableResult> TYdbStatsUploader::SetupHistoryTable() const
{
    return DbStorage->GetHistoryTables().Apply(
        [=] (const auto& future) {
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

            return SetupTable(tableName, HistoryTableScheme).Apply(
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
        [=] (const auto& future) {
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
    auto diff = CheckForAlter(existingScheme.Columns, scheme.Columns);
    if (FAILED(diff.Error.GetCode())) {
        return MakeFuture(diff.Error);
    }

    if (!diff.Columns) {
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
    const TVector<NYdb::TColumn>& existingColumns,
    const TVector<NYdb::TColumn>& newColumns) const
{
    THashMap<TString, EPrimitiveType> columnsSet;
    THashMap<TString, EPrimitiveType> diff;

    for (const auto& column: existingColumns) {
        auto typeParser = TTypeParser(column.Type);
        typeParser.OpenOptional();

        if (!columnsSet.emplace(column.Name, typeParser.GetPrimitive()).second) {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "duplicate field: " << column.Name);
        }

        typeParser.CloseOptional();
    }

    TStringBuilder newColumnList;

    for (const auto& column: newColumns) {
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

    STORAGE_INFO("New columns are: [" << newColumnList << ']')

    return std::move(diff);
}

////////////////////////////////////////////////////////////////////////////////

class TYdbStatsUploaderStub final
    : public IYdbVolumesStatsUploader
{
public:
    TFuture<NProto::TError> UploadStats(
        const TVector<TYdbRow>& stats,
        const TVector<TYdbBlobLoadMetricRow>& metrics) override
    {
        Y_UNUSED(stats);
        Y_UNUSED(metrics);
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

IYdbVolumesStatsUploaderPtr CreateYdbVolumesStatsUploader(
    TYdbStatsConfigPtr config,
    ILoggingServicePtr logging,
    IYdbStoragePtr dbStorage,
    TStatsTableSchemePtr statsTableScheme,
    TStatsTableSchemePtr historyTableScheme,
    TStatsTableSchemePtr archiveStatsTableScheme,
    TStatsTableSchemePtr metricsTableScheme)
{
    return std::make_unique<TYdbStatsUploader>(
        std::move(config),
        std::move(logging),
        std::move(dbStorage),
        std::move(statsTableScheme),
        std::move(historyTableScheme),
        std::move(archiveStatsTableScheme),
        std::move(metricsTableScheme));
}

IYdbVolumesStatsUploaderPtr CreateVolumesStatsUploaderStub()
{
    return std::make_unique<TYdbStatsUploaderStub>();
}

}   // namespace NCloud::NBlockStore::NYdbStats
