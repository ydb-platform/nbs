#pragma once

#include <cloud/blockstore/libs/storage/api/service.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TGetChangedBlocksCompanion
{
public:
    enum class EBehavior
    {
        ReplyError,
        DelegateRequest,
    };

private:
    EBehavior Behavior = EBehavior::ReplyError;
    NActors::TActorId Delegate;

public:
    TGetChangedBlocksCompanion() = default;

    void SetBehavior(EBehavior behavior, NActors::TActorId delegate);

    void HandleGetChangedBlocks(
        const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx) const;

private:
    void DoReplayError(
        const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx) const;
    void DoDelegateRequest(
        const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx) const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
