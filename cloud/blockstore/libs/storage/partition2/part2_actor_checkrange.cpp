#include "part2_actor.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/partition_common/actor_checkrange.h>

#include <util/generic/string.h>
#include <util/generic/xrange.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleCheckRange(
    const TEvService::TEvCheckRangeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto error = ValidateBlocksCount(
        msg->Record.GetBlocksCount(),
        Config->GetBytesPerStripe(),
        State->GetBlockSize(),
        Config->GetCheckRangeMaxRangeSize());

    if (HasError(error)) {
        auto response =
            std::make_unique<TEvService::TEvCheckRangeResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto response = std::make_unique<TEvService::TEvCheckRangeResponse>(
        MakeError(E_NOT_IMPLEMENTED));
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
