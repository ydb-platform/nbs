#include "sqlite_output.h"

#include <cloud/blockstore/libs/service/request.h>

#include <util/generic/serialized_enum.h>
#include <util/generic/yexception.h>

namespace NCloud::NBlockStore {

namespace {

constexpr ui64 RowsPerTransaction = 100000;

constexpr TStringBuf CreateRequestsTable = R"__(
    CREATE TABLE IF NOT EXISTS Requests (
        Id integer not null unique,
        At datetime not null,
        DiskId integer not null,
        RequestTypeId integer not null,
        StartBlock integer not null,
        EndBlock integer not null,
        DurationUs integer not null,
        PRIMARY KEY ("Id" AUTOINCREMENT)
    );
)__";

constexpr TStringBuf CreateVolumesTable = R"__(
    CREATE TABLE IF NOT EXISTS Disks (
        Id integer not null unique,
        DiskId text not null,
        PRIMARY KEY ("Id" AUTOINCREMENT)
    );
)__";

constexpr TStringBuf CreateRequestTypeTable = R"__(
    CREATE TABLE IF NOT EXISTS RequestTypes (
        Id integer not null unique,
        Request text not null,
        PRIMARY KEY ("Id")
    );
)__";

constexpr TStringBuf CreateRangesTable = R"__(
    CREATE TABLE IF NOT EXISTS Ranges (
        Id integer not null unique,
        DiskId text,
        StartAt datetime,
        EndAt datetime,
        StartBlock integer,
        EndBlock integer,
        PRIMARY KEY ("Id" AUTOINCREMENT)
    );
)__";

constexpr TStringBuf CreateFilteredView = R"__(
    drop view if exists FilteredRequest;
    create view FilteredRequest as
    SELECT
        rn.id as RangeId,
	    r.*
    from Requests as r
    INNER join Disks as d on d.Id == r.DiskId
    INNER join Ranges as rn
	    on d.DiskId == rn.DiskId and
	    (rn.StartBlock is null or rn.StartBlock <= r.EndBlock) and
	    (rn.EndBlock is null or rn.EndBlock >= r.StartBlock)
)__";

constexpr TStringBuf CreateRequestsIndex = R"__(
CREATE INDEX IF NOT EXISTS "disk_id" ON "Requests" (
	"DiskId"	ASC
);
)__";

constexpr TStringBuf AddDiskSql = R"__(
    INSERT INTO Disks
        (DiskId)
    VALUES
        (?);
)__";

constexpr TStringBuf AddRequestSql = R"__(
    INSERT INTO Requests
        (At, DiskId, RequestTypeId, StartBlock, EndBlock, DurationUs)
    VALUES
        (?, ?, ?, ?, ?, ?);
)__";

}   // namespace

///////////////////////////////////////////////////////////////////////////////

class TSqliteOutput::TTransaction
{
    sqlite3* Db = nullptr;

public:
    explicit TTransaction(sqlite3* db)
        : Db(db)
    {
        sqlite3_exec(Db, "BEGIN TRANSACTION", nullptr, nullptr, nullptr);
    }

    ~TTransaction()
    {
        sqlite3_exec(Db, "COMMIT TRANSACTION", nullptr, nullptr, nullptr);
    }
};

///////////////////////////////////////////////////////////////////////////////

TSqliteOutput::TSqliteOutput(const TString& filename)
{
    if (sqlite3_open(filename.c_str(), &Db) != 0) {
        ythrow yexception() << "can't open database: " << sqlite3_errmsg(Db);
    }

    CreateTables();
    AddRequestTypes();
    ReadDisks();

    Transaction = std::make_unique<TTransaction>(Db);
}

TSqliteOutput::~TSqliteOutput()
{
    Transaction.reset();
    Cout << "Total row count: " << TotalRowCount << Endl;

    if (Db) {
        sqlite3_finalize(AddDiskStmt);
        sqlite3_finalize(AddRequestStmt);
        sqlite3_close(Db);
    }
}

void TSqliteOutput::ProcessMessage(
    const NProto::TProfileLogRecord& message,
    EItemType itemType,
    int index)
{
    if (itemType != EItemType::Request) {
        return;
    }

    const NProto::TProfileLogRequestInfo& r = message.GetRequests(index);

    for (const auto& range: r.GetRanges()) {
        AddRequest(
            TInstant::FromValue(r.GetTimestampMcs()),
            GetVolumeId(message.GetDiskId()),
            r.GetRequestType(),
            TBlockRange64::WithLength(
                range.GetBlockIndex(),
                range.GetBlockCount()),
            TDuration::MicroSeconds(r.GetDurationMcs()));
    }
}

void TSqliteOutput::CreateTables()
{
    auto execSql = [&](const TStringBuf& sql)
    {
        char* error = nullptr;
        if (sqlite3_exec(Db, sql.data(), nullptr, nullptr, &error) != SQLITE_OK)
        {
            TString message(error);
            sqlite3_free(error);
            ythrow yexception() << message;
        }
    };
    execSql(CreateRequestsTable);
    execSql(CreateVolumesTable);
    execSql(CreateRequestTypeTable);
    execSql(CreateRangesTable);
    execSql(CreateFilteredView);
    execSql(CreateRequestsIndex);

    auto createStmt = [&](const TStringBuf& sql, sqlite3_stmt** stmt)
    {
        if (sqlite3_prepare_v2(Db, sql.data(), -1, stmt, nullptr) != SQLITE_OK)
        {
            ythrow yexception() << "Prepare error: " << sqlite3_errmsg(Db);
        }
    };

    createStmt(AddDiskSql, &AddDiskStmt);
    createStmt(AddRequestSql, &AddRequestStmt);
}

void TSqliteOutput::ReadDisks()
{
    const char* sql = "SELECT Id, DiskID FROM Disks;";
    sqlite3_stmt* stmt = nullptr;

    if (sqlite3_prepare_v2(Db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        ythrow yexception() << sqlite3_errmsg(Db);
    }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char* diskId = sqlite3_column_text(stmt, 1);
        TString d = TString(reinterpret_cast<const char*>(diskId));
        Volumes[d] = sqlite3_column_int(stmt, 0);
    }

    sqlite3_finalize(stmt);
}

void TSqliteOutput::AddRequestTypes()
{
    const char* sql =
        "INSERT OR REPLACE INTO RequestTypes(Id, Request) values(?, ?);";
    sqlite3_stmt* stmt = nullptr;

    if (sqlite3_prepare_v2(Db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        ythrow yexception() << sqlite3_errmsg(Db);
    }

    auto addRequestType = [&](int id, const TString& value)
    {
        sqlite3_reset(stmt);

        if (sqlite3_bind_int(stmt, 1, id) != SQLITE_OK) {
            ythrow yexception()
                << "Binding param error: " << sqlite3_errmsg(Db);
        }
        if (sqlite3_bind_text(
                stmt,
                2,
                value.c_str(),
                value.size(),
                SQLITE_TRANSIENT) != SQLITE_OK)
        {
            ythrow yexception()
                << "Binding param error: " << sqlite3_errmsg(Db);
        }
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            ythrow yexception() << "Step error: " << sqlite3_errmsg(Db);
        }
    };

    for (int i = 0; i < static_cast<int>(EBlockStoreRequest::MAX); ++i) {
        addRequestType(
            i,
            GetBlockStoreRequestName(static_cast<EBlockStoreRequest>(i)));
    }

    for (const auto& [id, name]: GetEnumNames<ESysRequestType>()) {
        addRequestType(static_cast<int>(id), name);
    }
}

ui64 TSqliteOutput::GetVolumeId(const TString& diskId)
{
    if (auto* volumeId = Volumes.FindPtr(diskId)) {
        return *volumeId;
    }

    sqlite3_reset(AddDiskStmt);

    if (sqlite3_bind_text(
            AddDiskStmt,
            1,
            diskId.c_str(),
            diskId.size(),
            SQLITE_TRANSIENT) != SQLITE_OK)
    {
        ythrow yexception() << "Binding param error: " << sqlite3_errmsg(Db);
    }

    if (sqlite3_step(AddDiskStmt) != SQLITE_DONE) {
        ythrow yexception() << "Step error: " << sqlite3_errmsg(Db);
    }

    int64_t lastRowId = sqlite3_last_insert_rowid(Db);
    Volumes[diskId] = lastRowId;
    return lastRowId;
}

void TSqliteOutput::AddRequest(
    TInstant timestamp,
    ui64 volumeId,
    ui64 requestTypeId,
    TBlockRange64 range,
    TDuration duration)
{
    sqlite3_reset(AddRequestStmt);

    auto bindInt = [&](int index, ui64 value)
    {
        if (sqlite3_bind_int64(AddRequestStmt, index, value) != SQLITE_OK) {
            ythrow yexception()
                << "Binding param error: " << sqlite3_errmsg(Db);
        }
    };

    auto bindDateTime = [&](int index, TInstant value)
    {
        auto str = value.ToString();
        if (sqlite3_bind_text(
                AddRequestStmt,
                index,
                str.c_str(),
                str.size(),
                SQLITE_TRANSIENT) != SQLITE_OK)
        {
            ythrow yexception()
                << "Binding param error: " << sqlite3_errmsg(Db);
        }
    };

    bindDateTime(1, timestamp);
    bindInt(2, volumeId);
    bindInt(3, requestTypeId);
    bindInt(4, range.Start);
    bindInt(5, range.End);
    bindInt(6, duration.MicroSeconds());

    if (sqlite3_step(AddRequestStmt) != SQLITE_DONE) {
        ythrow yexception() << "Step error: " << sqlite3_errmsg(Db);
    }

    AdvanceTransaction();
}

void TSqliteOutput::AdvanceTransaction()
{
    ++RowsInTransaction;
    ++TotalRowCount;

    if (RowsInTransaction >= RowsPerTransaction) {
        RowsInTransaction = 0;
        Transaction.reset();
        Transaction = std::make_unique<TTransaction>(Db);

        Cout << "Commit: " << TotalRowCount << Endl;
    }
}

}   // namespace NCloud::NBlockStore
