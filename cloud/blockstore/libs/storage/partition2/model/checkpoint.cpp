#include "checkpoint.h"

#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

NJson::TJsonValue TCheckpointStorage::AsJson() const
{
    NJson::TJsonValue result;
    for (const auto& checkpoint: Checkpoints) {
        NJson::TJsonValue json;
        try {
            // May throw.
            NProtobufJson::Proto2Json(checkpoint.second, json);
        } catch (...) {
        }
        result.AppendValue(json);
    }
    return result;
}

bool TCheckpointStorage::Add(const NProto::TCheckpointMeta& meta)
{
    TCheckpointMap::iterator it;
    bool inserted;

    std::tie(it, inserted) = Checkpoints.emplace(meta.GetCheckpointId(), meta);

    if (inserted) {
        InsertCommitId(meta.GetCommitId());
    }

    return inserted;
}

bool TCheckpointStorage::Remove(const TString& checkpointId)
{
    auto it = Checkpoints.find(checkpointId);
    if (it != Checkpoints.end()) {
        bool removed = RemoveCommitId(it->second.GetCommitId());
        Y_ABORT_UNLESS(removed);

        Checkpoints.erase(it);
        return true;
    }

    return false;
}

const NProto::TCheckpointMeta* TCheckpointStorage::Find(
    const TString& checkpointId) const
{
    auto it = Checkpoints.find(checkpointId);
    if (it != Checkpoints.end()) {
        return &it->second;
    }

    return nullptr;
}

ui64 TCheckpointStorage::GetCommitId(const TString& checkpointId) const
{
    auto it = Checkpoints.find(checkpointId);
    if (it != Checkpoints.end()) {
        return it->second.GetCommitId();
    }

    return 0;
}

ui64 TCheckpointStorage::RebaseCommitId(ui64 commitId) const
{
    auto it = LowerBound(CommitIds.begin(), CommitIds.end(), commitId);
    if (it != CommitIds.end()) {
        return *it;
    }

    return InvalidCommitId;
}

void TCheckpointStorage::InsertCommitId(ui64 commitId)
{
    auto it = LowerBound(CommitIds.begin(), CommitIds.end(), commitId);
    CommitIds.insert(it, commitId);
}

bool TCheckpointStorage::RemoveCommitId(ui64 commitId)
{
    auto it = LowerBound(CommitIds.begin(), CommitIds.end(), commitId);
    if (it != CommitIds.end() && *it == commitId) {
        CommitIds.erase(it);
        return true;
    }

    return false;
}

TVector<NProto::TCheckpointMeta> TCheckpointStorage::Get() const
{
    TVector<NProto::TCheckpointMeta> r(Reserve(Checkpoints.size()));
    for (const auto& i: Checkpoints) {
        r.push_back(i.second);
    }
    return r;
}

const TVector<ui64>& TCheckpointStorage::GetCommitIds() const
{
    return CommitIds;
}

void TCheckpointStorage::SetCheckpointBlockCount(ui64 c)
{
    CheckpointBlockCount = c;
}

ui64 TCheckpointStorage::GetCheckpointBlockCount() const
{
    return CheckpointBlockCount;
}

ui64 TCheckpointStorage::GetCount() const
{
    return Checkpoints.size();
}

bool TCheckpointStorage::IsEmpty() const
{
    return Checkpoints.empty();
}

////////////////////////////////////////////////////////////////////////////////

void TCheckpointsToDelete::Put(ui64 commitId, TCheckpointInfo info)
{
    CommitId2Info[commitId] = std::move(info);
    CommitIds.push_back(commitId);
}

TVector<TPartialBlobId> TCheckpointsToDelete::DeleteNextCheckpoint()
{
    if (CommitIds.size()) {
        auto cid = CommitIds.front();
        auto& info = CommitId2Info.at(cid);

        Y_ABORT_UNLESS(
            info.Idx == info.BlobIds.size(),
            "%u != %lu",
            info.Idx,
            info.BlobIds.size());

        auto ret = std::move(info.BlobIds);

        CommitId2Info.erase(cid);
        CommitIds.pop_front();

        return ret;
    }

    return {};
}

TVector<TPartialBlobId> TCheckpointsToDelete::ExtractBlobsToCleanup(
    size_t limit,
    ui64* commitId)
{
    if (CommitIds.empty()) {
        return {};
    }

    const auto cid = CommitIds.front();
    auto& info = CommitId2Info.at(cid);

    const auto cap = Min(limit, info.BlobIds.size() - info.Idx);

    TVector<TPartialBlobId> result(Reserve(cap));

    while (result.size() < cap) {
        result.push_back(info.BlobIds[info.Idx++]);
    }

    if (info.Idx == info.BlobIds.size()) {
        *commitId = cid;
    }

    return result;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
