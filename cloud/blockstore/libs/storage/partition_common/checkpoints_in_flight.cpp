#include "checkpoints_in_flight.h"

#include <cloud/blockstore/libs/storage/core/transaction.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TCheckpointsInFlight::TCheckpointsInFlight() = default;

TCheckpointsInFlight::~TCheckpointsInFlight() = default;

bool TCheckpointsInFlight::AddTx(
    const TString& checkpointId,
    TTxPtr transaction)
{
    return AddTx(checkpointId, std::move(transaction), 0);
}

bool TCheckpointsInFlight::AddTx(
    const TString& checkpointId,
    TTxPtr transaction,
    ui64 commitId)
{
    bool inserted = PendingTransactions
                        .emplace(
                            checkpointId,
                            TCheckpointTransactionToCommitId{
                                .Transaction = std::move(transaction),
                                .CommitId = commitId})
                        .second;
    if (!inserted) {
        return false;
    }

    // CommitId can be 0 if it is a DeleteCheckpoint transaction.
    if (commitId) {
        CommitIdQueue.Enqueue(checkpointId, commitId);
    }

    return true;
}

TCheckpointsInFlight::TTxPtr TCheckpointsInFlight::GetTx(
    const TString& checkpointId,
    ui64 commitId)
{
    auto it = PendingTransactions.find(checkpointId);
    if (it != PendingTransactions.end()) {
        auto& [tx, txCommitId] = it->second;

        if (txCommitId < commitId) {
            return std::move(tx);
        }
    }
    return {};
}

TCheckpointsInFlight::TTxPtr TCheckpointsInFlight::GetTx(ui64 commitId)
{
    TString checkpointId;
    while (checkpointId = CommitIdQueue.Dequeue(commitId)) {
        auto tx = GetTx(checkpointId, commitId);
        if (tx) {
            return tx;
        }
    }
    return {};
}

void TCheckpointsInFlight::PopTx(const TString& checkpointId)
{
    auto it = PendingTransactions.find(checkpointId);
    if (it != PendingTransactions.end()) {
        PendingTransactions.erase(it);
    }
}

bool TCheckpointsInFlight::HasCheckpoint(const TString& checkpointId) const
{
    return PendingTransactions.contains(checkpointId);
}

void TCheckpointsInFlight::GetCommitIds(TVector<ui64>& commitIds) const
{
    for (const auto& [_, txPair]: PendingTransactions) {
        const auto& txCommitId = txPair.CommitId;
        if (!txCommitId) {
            continue;
        }
        commitIds.push_back(txCommitId);
    }
}

ui64 TCheckpointsInFlight::GetMinCommitId() const
{
    ui64 minCommitId = Max<ui64>();
    for (const auto& [_, txPair]: PendingTransactions) {
        const auto& txCommitId = txPair.CommitId;
        if (!txCommitId) {
            continue;
        }
        minCommitId = Min(minCommitId, txCommitId);
    }
    return minCommitId;
}

ui64 TCheckpointsInFlight::GetMaxCommitId() const
{
    ui64 maxCommitId = 0;
    for (const auto& [_, txPair]: PendingTransactions) {
        const auto& txCommitId = txPair.CommitId;
        if (!txCommitId) {
            continue;
        }
        maxCommitId = Max(maxCommitId, txCommitId);
    }
    return maxCommitId;
}

}   // namespace NCloud::NBlockStore::NStorage
