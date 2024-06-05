#pragma once

#include <cloud/blockstore/libs/storage/api/service.h>

#include <library/cpp/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Helps to process the TEvGetChangedBlocksRequest message. It can either
// respond to it with an error, or redirect it to another actor.
class TGetChangedBlocksCompanion
{
private:
    NActors::TActorId Delegate;

public:
    TGetChangedBlocksCompanion() = default;

    void SetDelegate(NActors::TActorId delegate);

    void HandleGetChangedBlocks(
        const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx) const;

private:
    void DoReplyError(
        const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx) const;
    void DoDelegateRequest(
        const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx) const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
