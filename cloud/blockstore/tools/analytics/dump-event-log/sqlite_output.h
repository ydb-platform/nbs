#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/tools/analytics/libs/event-log/dump.h>

#include <contrib/libs/sqlite3/sqlite3.h>

#include <util/datetime/base.h>
#include <util/generic/map.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

struct TWriteRequest {
    TInstant Timestamp;
    TString DiskId;
    TDuration Duration;
    TDuration Postponed;
    ui32 BlockCount;
};

class TSqliteOutput
{
    class TTransaction;

    using TReplicaChecksums =
        google::protobuf::RepeatedPtrField<NProto::TReplicaChecksum>;

    sqlite3* Db = nullptr;
    sqlite3_stmt* AddDiskStmt = nullptr;
    sqlite3_stmt* AddRequestStmt = nullptr;
    sqlite3_stmt* AddChecksumStmt = nullptr;
    TMap<TString, ui64> Volumes;
    ui64 RowsInTransaction = 0;
    ui64 TotalRowCount = 0;
    std::unique_ptr<TTransaction> Transaction;

public:
    explicit TSqliteOutput(const TString& filename);
    ~TSqliteOutput();

    TSqliteOutput(const TSqliteOutput&) = delete;
    TSqliteOutput(TSqliteOutput&&) = delete;
    TSqliteOutput& operator=(const TSqliteOutput&) = delete;
    TSqliteOutput& operator=(TSqliteOutput&&) = delete;

    void ProcessMessage(
        const NProto::TProfileLogRecord& message,
        EItemType itemType,
        int index);

    void SelectWriteRequestsDescending(
        std::function<void(TWriteRequest)> callback);

    void DumpDataset();

private:
    void CreateTables();
    void ReadDisks();
    void AddRequestTypes();
    void AddZeroChecksumsTypes();
    void AddBlocksSequence();

    ui64 GetVolumeId(const TString& diskId);
    ui64 AddRequest(
        TInstant timestamp,
        ui64 volumeId,
        ui64 requestTypeId,
        TBlockRange64 range,
        TDuration duration,
        TDuration postponed);
    void AddChecksums(
        ui64 requestId,
        TBlockRange64 blockRange,
        const TReplicaChecksums& replicaChecksums);

    void AdvanceTransaction();
};

}   // namespace NCloud::NBlockStore
