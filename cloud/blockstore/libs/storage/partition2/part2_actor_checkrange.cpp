#include "part2_actor.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/partition_common/actor_checkrange.h>

#include <util/generic/string.h>
#include <util/generic/xrange.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

void TPartitionActor::HandleCheckRange(
    const TEvService::TEvCheckRangeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (auto error = ValidateBlocksCount(
            msg->Record.GetBlocksCount(),
            Config->GetBytesPerStripe(),
            State->GetBlockSize(),
            Config->GetCheckRangeMaxRangeSize()))
    {
        auto response =
            std::make_unique<TEvService::TEvCheckRangeResponse>(*error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto actorId = NCloud::Register<TCheckRangeActor>(
        ctx,
        SelfId(),
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount(),
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext));
    Actors.insert(actorId);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
