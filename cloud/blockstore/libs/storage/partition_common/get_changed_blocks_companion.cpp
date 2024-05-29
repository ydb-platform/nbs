#include "get_changed_blocks_companion.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TGetChangedBlocksCompanion::SetDelegate(
    NActors::TActorId delegate)
{
    Delegate = delegate;
}

void TGetChangedBlocksCompanion::HandleGetChangedBlocks(
    const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx) const
{
    if (Delegate) {
        DoDelegateRequest(ev, ctx);
    } else {
        DoReplyError(ev, ctx);
    }
}

void TGetChangedBlocksCompanion::DoReplyError(
    const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx) const
{
    auto response = std::make_unique<TEvService::TEvGetChangedBlocksResponse>(
        MakeError(E_ARGUMENT, "GetChangedBlocks not supported"));
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TGetChangedBlocksCompanion::DoDelegateRequest(
    const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx) const
{
    STORAGE_CHECK_PRECONDITION(Delegate);

    NActors::TActorId nondeliveryActor = ev->GetForwardOnNondeliveryRecipient();
    auto message = std::make_unique<IEventHandle>(
        Delegate,
        ev->Sender,
        ev->ReleaseBase().Release(),
        ev->Flags,
        ev->Cookie,
        ev->Flags & NActors::IEventHandle::FlagForwardOnNondelivery
            ? &nondeliveryActor
            : nullptr);
    ctx.Send(std::move(message));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
