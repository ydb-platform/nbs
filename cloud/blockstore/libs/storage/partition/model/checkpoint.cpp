#include "checkpoint.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

NJson::TJsonValue TCheckpoint::AsJson() const
{
    NJson::TJsonValue json;
    json["CheckpointId"] = CheckpointId;
    json["CommitId"] = CommitId;
    json["IdempotenceId"] = IdempotenceId;
    json["DateCreated"] = DateCreated.MicroSeconds();
    try {
        NJson::TJsonValue stats;
        // May throw.
        NProtobufJson::Proto2Json(Stats, stats);
        json["Stats"] = std::move(stats);
    } catch (...) {
    }
    return json;
}

bool TCheckpointStore::Add(const TCheckpoint& checkpoint)
{
    if (AddCheckpointMapping(checkpoint)) {
        Items.insert_unique(checkpoint);
        InsertCommitId(checkpoint.CommitId);
        return true;
    }
    return false;
}

void TCheckpointStore::Add(TVector<TCheckpoint>& checkpoints)
{
    for (const auto& checkpoint: checkpoints) {
        bool success = Add(checkpoint);
        Y_ABORT_UNLESS(success);
    }
}

bool TCheckpointStore::AddCheckpointMapping(const TCheckpoint& checkpoint)
{
    if (Items.find(checkpoint.CheckpointId) == Items.end()) {
        return CheckpointId2CommitId
            .try_emplace(checkpoint.CheckpointId, checkpoint.CommitId)
            .second;
    }
    return false;
}

void TCheckpointStore::SetCheckpointMappings(
    const THashMap<TString, ui64>& checkpointId2CommitId)
{
    CheckpointId2CommitId = checkpointId2CommitId;
}

bool TCheckpointStore::Delete(const TString& checkpointId)
{
    auto it = Items.find(checkpointId);
    if (it != Items.end()) {
        bool removed = RemoveCommitId(it->CommitId);
        Y_ABORT_UNLESS(removed);

        Items.erase(it);
        return true;
    }
    return false;
}

bool TCheckpointStore::DeleteCheckpointMapping(const TString& checkpointId)
{
    return CheckpointId2CommitId.erase(checkpointId);
}

ui64 TCheckpointStore::GetCommitId(
    const TString& checkpointId,
    bool allowCheckpointWithoutData) const
{
    auto it = Items.find(checkpointId);
    if (it != Items.end()) {
        return it->CommitId;
    }

    auto it2 = CheckpointId2CommitId.find(checkpointId);
    if (allowCheckpointWithoutData && it2 != CheckpointId2CommitId.end()) {
        return it2->second;
    }
    return 0;
}

TString TCheckpointStore::GetIdempotenceId(const TString& checkpointId) const
{
    auto it = Items.find(checkpointId);
    if (it != Items.end()) {
        return it->IdempotenceId;
    }
    return {};
}

TVector<TCheckpoint> TCheckpointStore::Get() const
{
    TVector<TCheckpoint> result(Reserve(Items.size()));
    for (const auto& checkpoint: Items) {
        result.push_back(checkpoint);
    }
    return result;
}

const TCheckpoint* TCheckpointStore::GetLast() const
{
    const TCheckpoint* last = nullptr;

    for (const auto& checkpoint: Items) {
        if (!last || last->CommitId < checkpoint.CommitId) {
            last = &checkpoint;
        }
    }

    return last;
}

const THashMap<TString, ui64>& TCheckpointStore::GetMapping() const
{
    return CheckpointId2CommitId;
}

ui64 TCheckpointStore::GetMinCommitId() const
{
    if (CommitIds.empty()) {
        return Max();
    }
    return CommitIds.front();
}

void TCheckpointStore::GetCommitIds(TVector<ui64>& result) const
{
    for (auto commitId: CommitIds) {
        result.push_back(commitId);
    }
}

NJson::TJsonValue TCheckpointStore::AsJson() const
{
    NJson::TJsonValue json;
    for (const auto& checkpoint: Items) {
        json.AppendValue(checkpoint.AsJson());
    }
    return json;
}

void TCheckpointStore::InsertCommitId(ui64 commitId)
{
    auto it = LowerBound(CommitIds.begin(), CommitIds.end(), commitId);
    CommitIds.insert(it, commitId);
}

bool TCheckpointStore::RemoveCommitId(ui64 commitId)
{
    auto it = LowerBound(CommitIds.begin(), CommitIds.end(), commitId);
    if (it != CommitIds.end() && *it == commitId) {
        CommitIds.erase(it);
        return true;
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

void TCheckpointQueue::Enqueue(const TString& checkpointId, ui64 commitId)
{
    Queue.emplace_back(commitId, checkpointId);
}

TString TCheckpointQueue::Dequeue(ui64 commitId)
{
    if (Queue) {
        auto& next = Queue.front();
        if (next.first < commitId) {
            auto retval = std::move(next.second);
            Queue.pop_front();
            return retval;
        }
    }
    return {};
}

bool TCheckpointQueue::Empty() const
{
    return Queue.empty();
}

////////////////////////////////////////////////////////////////////////////////

void TCheckpointsInFlight::AddTx(
    const TString& checkpointId,
    TTxPtr transaction)
{
    auto& queue = PendingTransactions[checkpointId];
    queue.emplace_back(std::move(transaction), 0);
}

void TCheckpointsInFlight::AddTx(
    const TString& checkpointId,
    TTxPtr transaction,
    ui64 commitId)
{
    auto& queue = PendingTransactions[checkpointId];
    queue.emplace_back(std::move(transaction), commitId);

    if (queue.size() == 1) {
        CommitIdQueue.Enqueue(checkpointId, commitId);
    }
}

TCheckpointsInFlight::TTxPtr TCheckpointsInFlight::GetTx(
    const TString& checkpointId,
    ui64 commitId)
{
    auto it = PendingTransactions.find(checkpointId);
    if (it != PendingTransactions.end()) {
        auto& queue = it->second;

        if (queue) {
            auto& next = queue.front();
            if (next.second < commitId) {
                return std::move(next.first);
            }
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
        auto& queue = it->second;

        Y_ABORT_UNLESS(queue);
        queue.pop_front();

        if (queue) {
            auto& next = queue.front();
            if (next.second) {
                CommitIdQueue.Enqueue(checkpointId, next.second);
            }
        } else {
            PendingTransactions.erase(it);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
