#include "sqlite_output.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/service/request.h>

#include <util/generic/serialized_enum.h>
#include <util/generic/yexception.h>

namespace NCloud::NBlockStore {

namespace {

constexpr ui64 RowsPerTransaction = 100000;

constexpr TStringBuf CreateRequestsTable = R"__(
    CREATE TABLE IF NOT EXISTS Requests (
        Id integer not null unique,
        AtUs integer not null,
        DiskId integer not null,
        RequestTypeId integer not null,
        StartBlock integer not null,
        EndBlock integer not null,
        DurationUs integer not null,
        PostponedUs integer not null,
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

constexpr TStringBuf CreateChecksumsTable = R"__(
    CREATE TABLE IF NOT EXISTS Checksums (
        Id integer not null unique,
        RequestId integer not null,
        StartBlock integer not null,
        EndBlock integer not null,
        Checksum0 integer not null,
        Checksum1 integer not null,
        Checksum2 integer not null,
        PRIMARY KEY ("Id" AUTOINCREMENT)
    );
)__";

constexpr TStringBuf CreateZeroBlockChecksumsTable = R"__(
    CREATE TABLE IF NOT EXISTS ZeroBlockChecksums (
        Id integer not null unique,
        Size integer not null,
        BlockCount integer not null,
        Checksum integer not null,
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

constexpr TStringBuf CreateBlocksSequenceTable = R"__(
    CREATE TABLE IF NOT EXISTS BlocksSequence (
        BlockIndx integer not null unique,
        PRIMARY KEY ("BlockIndx")
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
    "DiskId"    ASC
);
CREATE INDEX IF NOT EXISTS "request_id" ON "Checksums" (
    "RequestId"    ASC
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
        (AtUs, DiskId, RequestTypeId, StartBlock, EndBlock, DurationUs, PostponedUs)
    VALUES
        (?, ?, ?, ?, ?, ?, ?);
)__";

constexpr TStringBuf AddChecksumSql = R"__(
    INSERT INTO Checksums
        (RequestId, StartBlock, EndBlock, Checksum0, Checksum1, Checksum2)
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

    Transaction = std::make_unique<TTransaction>(Db);

    CreateTables();
    AddZeroChecksumsTypes();
    AddRequestTypes();
    AddBlocksSequence();
    ReadDisks();

    Transaction.reset();
    Transaction = std::make_unique<TTransaction>(Db);
}

TSqliteOutput::~TSqliteOutput()
{
    Transaction.reset();
    Cout << "Total row count: " << TotalRowCount << Endl;

    if (Db) {
        sqlite3_finalize(AddDiskStmt);
        sqlite3_finalize(AddRequestStmt);
        sqlite3_finalize(AddChecksumStmt);
        sqlite3_close(Db);
    }
}

void TSqliteOutput::ProcessRequest(
    const TDiskInfo& diskInfo,
    TInstant timestamp,
    ui32 requestType,
    TBlockRange64 blockRange,
    TDuration duration,
    TDuration postponed,
    const TReplicaChecksums& replicaChecksums,
    const TInflightInfo& inflightInfo)
{
    Y_UNUSED(inflightInfo);

    const ui64 requestId = AddRequest(
        timestamp,
        GetVolumeId(diskInfo.DiskId),
        requestType,
        blockRange,
        duration,
        postponed);

    AddChecksums(requestId, blockRange, replicaChecksums);

    AdvanceTransaction();
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
    execSql(CreateBlocksSequenceTable);
    execSql(CreateChecksumsTable);
    execSql(CreateZeroBlockChecksumsTable);
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
    createStmt(AddChecksumSql, &AddChecksumStmt);
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

void TSqliteOutput::AddZeroChecksumsTypes()
{
    constexpr ui32 TotalBlockCount = 1024;
    constexpr ui32 BlockSize = 4096;

    const char* sql =
        "INSERT OR REPLACE INTO ZeroBlockChecksums(Size, BlockCount, Checksum) "
        "values (?, ?, ?);";

    sqlite3_stmt* stmt = nullptr;

    if (sqlite3_prepare_v2(Db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        ythrow yexception() << sqlite3_errmsg(Db);
    }

    auto addZeroChecksumType =
        [&](ui32 size, ui32 blockCount, const ui32& checksum)
    {
        sqlite3_reset(stmt);

        const bool ok = sqlite3_bind_int64(stmt, 1, size) == SQLITE_OK &&
                        sqlite3_bind_int64(stmt, 2, blockCount) == SQLITE_OK &&
                        sqlite3_bind_int64(stmt, 3, checksum) == SQLITE_OK;
        if (!ok) {
            ythrow yexception()
                << "Binding param error: " << sqlite3_errmsg(Db);
        }
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            Cerr << "Step error: " << sqlite3_errmsg(Db) << Endl;
        }
    };

    const TString zero(TotalBlockCount * BlockSize, '\0');
    for (ui32 blockCount = 1; blockCount <= TotalBlockCount; blockCount++) {
        const ui32 size = blockCount * BlockSize;
        TBlockChecksum calc;
        calc.Extend(zero.data(), size);
        addZeroChecksumType(size, blockCount, calc.GetValue());
    }
}

void TSqliteOutput::AddBlocksSequence()
{
    const char* sql =
        "INSERT OR REPLACE INTO BlocksSequence(BlockIndx) VALUES(?);";
    sqlite3_stmt* stmt = nullptr;

    if (sqlite3_prepare_v2(Db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        ythrow yexception() << sqlite3_errmsg(Db);
    }

    auto addBlock = [&](int blockIndex)
    {
        sqlite3_reset(stmt);

        if (sqlite3_bind_int(stmt, 1, blockIndex) != SQLITE_OK) {
            ythrow yexception()
                << "Binding param error: " << sqlite3_errmsg(Db);
        }
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            Cerr << "Step error: " << sqlite3_errmsg(Db) << Endl;
        }
    };

    for (int i = 0; i < 1024; ++i) {
        addBlock(i);
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

ui64 TSqliteOutput::AddRequest(
    TInstant timestamp,
    ui64 volumeId,
    ui64 requestTypeId,
    TBlockRange64 range,
    TDuration duration,
    TDuration postponed)
{
    sqlite3_reset(AddRequestStmt);

    auto bindInt = [&](int index, ui64 value)
    {
        if (sqlite3_bind_int64(AddRequestStmt, index, value) != SQLITE_OK) {
            ythrow yexception()
                << "Binding param error: " << sqlite3_errmsg(Db);
        }
    };

    bindInt(1, timestamp.MicroSeconds());
    bindInt(2, volumeId);
    bindInt(3, requestTypeId);
    bindInt(4, range.Start);
    bindInt(5, range.End);
    bindInt(6, duration.MicroSeconds());
    bindInt(7, postponed.MicroSeconds());

    if (sqlite3_step(AddRequestStmt) != SQLITE_DONE) {
        ythrow yexception() << "Step error: " << sqlite3_errmsg(Db);
    }
    return sqlite3_last_insert_rowid(Db);
}

void TSqliteOutput::AddChecksums(
    ui64 requestId,
    TBlockRange64 blockRange,
    const TReplicaChecksums& replicaChecksums)
{
    if (replicaChecksums.empty()) {
        return;
    }

    auto bindInt = [&](int index, ui64 value)
    {
        if (sqlite3_bind_int64(AddChecksumStmt, index, value) != SQLITE_OK) {
            ythrow yexception()
                << "Binding param error: " << sqlite3_errmsg(Db);
        }
    };

    TMap<TBlockRange64, std::array<ui32, 3>, TBlockRangeComparator>
        rangeChecksums;
    for (const NProto::TReplicaChecksum& replicaChecksum: replicaChecksums) {
        for (int i = 0; i < replicaChecksum.GetChecksums().size(); ++i) {
            const ui32 replicaIndx = replicaChecksum.GetReplicaId();
            const TBlockRange64 range =
                (replicaChecksum.GetChecksums().size() == 1)
                    ? blockRange
                    : TBlockRange64::MakeOneBlock(blockRange.Start + i);
            if (replicaIndx < 0 || replicaIndx > 2) {
                Cerr << "Invalid replica id: " << replicaIndx << Endl;
            } else {
                rangeChecksums[range][replicaIndx] =
                    replicaChecksum.GetChecksums(i);
            }
        }
    }

    for (const auto& [range, checksums]: rangeChecksums) {
        sqlite3_reset(AddChecksumStmt);

        bindInt(1, requestId);
        bindInt(2, range.Start);
        bindInt(3, range.End);
        bindInt(4, checksums[0]);
        bindInt(5, checksums[1]);
        bindInt(6, checksums[2]);
    }

    if (sqlite3_step(AddChecksumStmt) != SQLITE_DONE) {
        ythrow yexception() << "Step error: " << sqlite3_errmsg(Db);
    }
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
