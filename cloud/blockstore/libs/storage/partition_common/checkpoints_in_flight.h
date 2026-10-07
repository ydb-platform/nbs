#pragma once

#include "model/checkpoint.h"

#include <memory>

namespace NCloud::NBlockStore::NStorage {

struct ITransactionBase;

////////////////////////////////////////////////////////////////////////////////

class TCheckpointsInFlight
{
    using TTxPtr = std::unique_ptr<ITransactionBase>;
    using TTxQueue = TDeque<std::pair<TTxPtr, ui64>>;

    struct TCheckpointTransactionToCommitId
    {
        TTxPtr Transaction;
        ui64 CommitId;
    };

private:
    THashMap<TString, TCheckpointTransactionToCommitId> PendingTransactions;
    TCheckpointQueue CommitIdQueue;

public:
    TCheckpointsInFlight();
    ~TCheckpointsInFlight();

    bool AddTx(const TString& checkpointId, TTxPtr transaction);
    bool AddTx(const TString& checkpointId, TTxPtr transaction, ui64 commitId);

    TTxPtr GetTx(const TString& checkpointId, ui64 commitId);
    TTxPtr GetTx(ui64 commitId);

    void PopTx(const TString& checkpointId);

    [[nodiscard]] bool HasCheckpoint(const TString& checkpointId) const;

    void GetCommitIds(TVector<ui64>& commitIds) const;

    [[nodiscard]] ui64 GetMinCommitId() const;
    [[nodiscard]] ui64 GetMaxCommitId() const;
};

}   // namespace NCloud::NBlockStore::NStorage
