#include "checkpoint.h"

#include <cloud/storage/core/libs/tablet/model/commit.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TCheckpoint* TCheckpointStore::CreateCheckpoint(
    const NProto::TCheckpoint& proto)
{
    auto checkpoint = std::make_unique<TCheckpoint>(proto);

    Checkpoints.PushBack(checkpoint.get());
    CheckpointById.emplace(checkpoint->GetCheckpointId(), checkpoint.get());

    if (!checkpoint->GetDeleted()) {
        CommitIds.insert(checkpoint->GetCommitId());
    }

    return checkpoint.release();
}

void TCheckpointStore::MarkCheckpointDeleted(TCheckpoint* checkpoint)
{
    checkpoint->SetDeleted(true);

    CommitIds.erase(checkpoint->GetCommitId());
}

void TCheckpointStore::RemoveCheckpoint(TCheckpoint* checkpoint)
{
    Y_VERIFY(checkpoint->GetDeleted());

    std::unique_ptr<TCheckpoint> holder(checkpoint);
    checkpoint->Unlink();

    CheckpointById.erase(checkpoint->GetCheckpointId());
}

TVector<TCheckpoint*> TCheckpointStore::GetCheckpoints() const
{
    TVector<TCheckpoint*> result;
    for (const auto& checkpoint: Checkpoints) {
        result.push_back(const_cast<TCheckpoint*>(&checkpoint));
    }

    return result;
}

TCheckpoint* TCheckpointStore::FindCheckpoint(const TString& checkpointId) const
{
    auto it = CheckpointById.find(checkpointId);
    if (it != CheckpointById.end()) {
        return it->second;
    }

    return nullptr;
}

ui64 TCheckpointStore::FindCheckpoint(ui64 nodeId, ui64 commitId) const
{
    // TODO
    Y_UNUSED(nodeId);

    auto it = CommitIds.lower_bound(commitId);
    if (it != CommitIds.end()) {
        return *it;
    }

    return InvalidCommitId;
}

}   // namespace NCloud::NFileStore::NStorage
