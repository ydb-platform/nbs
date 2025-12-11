#include "tablet_state_impl.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletState::LoadCheckpoints(
    const TVector<NProto::TCheckpoint>& checkpoints)
{
    for (const auto& proto: checkpoints) {
        auto* checkpoint = Impl->Checkpoints.CreateCheckpoint(proto);
        Y_ABORT_UNLESS(checkpoint);
    }
}

TVector<TCheckpoint*> TIndexTabletState::GetCheckpoints() const
{
    return Impl->Checkpoints.GetCheckpoints();
}

TCheckpoint* TIndexTabletState::FindCheckpoint(
    const TString& checkpointId) const
{
    return Impl->Checkpoints.FindCheckpoint(checkpointId);
}

ui64 TIndexTabletState::GetReadCommitId(const TString& checkpointId) const
{
    if (!checkpointId) {
        return GetCurrentCommitId();
    }

    auto* checkpoint = Impl->Checkpoints.FindCheckpoint(checkpointId);
    if (checkpoint && !checkpoint->GetDeleted()) {
        return checkpoint->GetCommitId();
    }

    return InvalidCommitId;
}

TCheckpoint* TIndexTabletState::CreateCheckpoint(
    TIndexTabletDatabase& db,
    const TString& checkpointId,
    ui64 nodeId,
    ui64 commitId)
{
    NProto::TCheckpoint proto;
    proto.SetCheckpointId(checkpointId);
    proto.SetNodeId(nodeId);
    proto.SetCommitId(commitId);

    db.WriteCheckpoint(proto);

    auto* checkpoint = Impl->Checkpoints.CreateCheckpoint(proto);
    Y_ABORT_UNLESS(checkpoint);

    Impl->FreshBytes.OnCheckpoint(commitId);

    return checkpoint;
}

void TIndexTabletState::MarkCheckpointDeleted(
    TIndexTabletDatabase& db,
    TCheckpoint* checkpoint)
{
    Impl->Checkpoints.MarkCheckpointDeleted(checkpoint);

    db.WriteCheckpoint(*checkpoint);
}

void TIndexTabletState::RemoveCheckpointNodes(
    TIndexTabletDatabase& db,
    TCheckpoint* checkpoint,
    const TVector<ui64>& nodes)
{
    ui64 commitId = checkpoint->GetCommitId();
    for (ui64 nodeId: nodes) {
        db.DeleteCheckpointNode(commitId, nodeId);
    }

    DecrementCheckpointNodesCount(db, nodes.size());
}

void TIndexTabletState::RemoveCheckpointBlob(
    TIndexTabletDatabase& db,
    TCheckpoint* checkpoint,
    ui32 rangeId,
    const TPartialBlobId& blobId)
{
    db.DeleteCheckpointBlob(checkpoint->GetCommitId(), rangeId, blobId);
    DecrementCheckpointBlobsCount(db);
}

void TIndexTabletState::RemoveCheckpoint(
    TIndexTabletDatabase& db,
    TCheckpoint* checkpoint)
{
    db.DeleteCheckpoint(checkpoint->GetCheckpointId());

    Impl->Checkpoints.RemoveCheckpoint(checkpoint);
}

void TIndexTabletState::AddCheckpointNode(
    TIndexTabletDatabase& db,
    ui64 checkpointId,
    ui64 nodeId)
{
    db.WriteCheckpointNode(checkpointId, nodeId);
    IncrementCheckpointNodesCount(db);
}

void TIndexTabletState::AddCheckpointBlob(
    TIndexTabletDatabase& db,
    ui64 checkpointId,
    ui32 rangeId,
    const TPartialBlobId& blobId)
{
    db.WriteCheckpointBlob(checkpointId, rangeId, blobId);
    IncrementCheckpointBlobsCount(db);
}

}   // namespace NCloud::NFileStore::NStorage
