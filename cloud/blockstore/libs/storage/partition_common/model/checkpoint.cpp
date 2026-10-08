#include "checkpoint.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>

namespace NCloud::NBlockStore::NStorage {

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
    } catch (...) {}
    return json;
}

bool TPartitionCheckpointStore::Add(const TCheckpoint& checkpoint)
{
    if (AddCheckpointMapping(checkpoint)) {
        Items.insert_unique(checkpoint);
        InsertCommitId(checkpoint.CommitId);
        return true;
    }
    return false;
}

void TPartitionCheckpointStore::Add(TVector<TCheckpoint>& checkpoints)
{
    for (const auto& checkpoint: checkpoints) {
        bool success = Add(checkpoint);
        Y_ABORT_UNLESS(success);
    }
}

bool TPartitionCheckpointStore::AddCheckpointMapping(
    const TCheckpoint& checkpoint)
{
    if (Items.find(checkpoint.CheckpointId) == Items.end()) {
        return CheckpointId2CommitId.try_emplace(checkpoint.CheckpointId, checkpoint.CommitId).second;
    }
    return false;
}

void TPartitionCheckpointStore::SetCheckpointMappings(
    const THashMap<TString, ui64>& checkpointId2CommitId)
{
    CheckpointId2CommitId = checkpointId2CommitId;
}

bool TPartitionCheckpointStore::Delete(const TString& checkpointId)
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

bool TPartitionCheckpointStore::DeleteCheckpointMapping(
    const TString& checkpointId)
{
    return CheckpointId2CommitId.erase(checkpointId);
}

ui64 TPartitionCheckpointStore::GetCommitId(
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

TString TPartitionCheckpointStore::GetIdempotenceId(
    const TString& checkpointId) const
{
    auto it = Items.find(checkpointId);
    if (it != Items.end()) {
        return it->IdempotenceId;
    }
    return {};
}

TVector<TCheckpoint> TPartitionCheckpointStore::Get() const
{
    TVector<TCheckpoint> result(Reserve(Items.size()));
    for (const auto& checkpoint: Items) {
        result.push_back(checkpoint);
    }
    return result;
}

const TCheckpoint* TPartitionCheckpointStore::GetLast() const
{
    const TCheckpoint* last = nullptr;

    for (const auto& checkpoint: Items) {
        if (!last || last->CommitId < checkpoint.CommitId) {
            last = &checkpoint;
        }
    }

    return last;
}

const THashMap<TString, ui64>& TPartitionCheckpointStore::GetMapping() const
{
    return CheckpointId2CommitId;
}

ui64 TPartitionCheckpointStore::GetMinCommitId() const
{
    if (CommitIds.empty()) {
        return Max();
    }
    return CommitIds.front();
}

ui64 TPartitionCheckpointStore::GetMaxCommitId() const
{
    if (CommitIds.empty()) {
        return 0;
    }
    return CommitIds.back();
}

void TPartitionCheckpointStore::GetCommitIds(TVector<ui64>& result) const
{
    for (ui64 commitId: CommitIds) {
        result.push_back(commitId);
    }
}

NJson::TJsonValue TPartitionCheckpointStore::AsJson() const
{
    NJson::TJsonValue json;
    for (const auto& checkpoint: Items) {
        json.AppendValue(checkpoint.AsJson());
    }
    return json;
}

void TPartitionCheckpointStore::InsertCommitId(ui64 commitId)
{
    auto it = LowerBound(CommitIds.begin(), CommitIds.end(), commitId);
    CommitIds.insert(it, commitId);
}

bool TPartitionCheckpointStore::RemoveCommitId(ui64 commitId)
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

}   // namespace NCloud::NBlockStore::NStorage
