#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/tablet.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

struct TCheckpoint
{
    TString CheckpointId;
    ui64 CommitId = 0;
    TString IdempotenceId;
    TInstant DateCreated;
    NProto::TPartitionStats Stats;

    TCheckpoint() = default;

    TCheckpoint(
        TString checkpointId,
        ui64 commitId,
        TString idempotenceId,
        TInstant dateCreated,
        NProto::TPartitionStats stats)
        : CheckpointId(std::move(checkpointId))
        , CommitId(commitId)
        , IdempotenceId(std::move(idempotenceId))
        , DateCreated(dateCreated)
        , Stats(std::move(stats))
    {}

    NJson::TJsonValue AsJson() const;
};

////////////////////////////////////////////////////////////////////////////////

class TCheckpointStore
{
    struct ExtractKey
    {
        const TString& operator()(const TCheckpoint& x) const
        {
            return x.CheckpointId;
        }
    };

    using TCheckpointMap = THashTable<
        TCheckpoint,
        TString,
        THash<TString>,
        ExtractKey,
        TEqualTo<TString>,
        std::allocator<TCheckpoint>>;

private:
    TCheckpointMap Items;
    THashMap<TString, ui64> CheckpointId2CommitId;
    TVector<ui64> CommitIds;

public:
    bool Add(const TCheckpoint& checkpoint);
    void Add(TVector<TCheckpoint>& checkpoints);
    bool Delete(const TString& checkpointId);

    bool AddCheckpointMapping(const TCheckpoint& checkpoint);
    void SetCheckpointMappings(
        const THashMap<TString, ui64>& checkpointId2CommitId);
    bool DeleteCheckpointMapping(const TString& checkpointId);

    ui64 GetCommitId(
        const TString& checkpointId,
        bool allowCheckpointWithoutData) const;
    TString GetIdempotenceId(const TString& checkpointId) const;

    TVector<TCheckpoint> Get() const;
    const TCheckpoint* GetLast() const;

    const THashMap<TString, ui64>& GetMapping() const;

    ui64 GetMinCommitId() const;

    void GetCommitIds(TVector<ui64>& result) const;

    NJson::TJsonValue AsJson() const;

    bool IsEmpty() const
    {
        return Items.empty();
    }

    ui64 GetCount() const
    {
        return Items.size();
    }

private:
    void InsertCommitId(ui64 commitId);
    bool RemoveCommitId(ui64 commitId);
};

////////////////////////////////////////////////////////////////////////////////

class TCheckpointQueue
{
    using TQueue = TDeque<std::pair<ui64, TString>>;

private:
    TQueue Queue;

public:
    void Enqueue(const TString& checkpointId, ui64 commitId);
    TString Dequeue(ui64 commitId);

    bool Empty() const;
};

////////////////////////////////////////////////////////////////////////////////

class TCheckpointsInFlight
{
    using TTxPtr = std::unique_ptr<ITransactionBase>;
    using TTxQueue = TDeque<std::pair<TTxPtr, ui64>>;

private:
    THashMap<TString, TTxQueue> PendingTransactions;
    TCheckpointQueue CommitIdQueue;

public:
    void AddTx(const TString& checkpointId, TTxPtr transaction);
    void AddTx(const TString& checkpointId, TTxPtr transaction, ui64 commitId);

    TTxPtr GetTx(const TString& checkpointId, ui64 commitId);
    TTxPtr GetTx(ui64 commitId);

    void PopTx(const TString& checkpointId);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
