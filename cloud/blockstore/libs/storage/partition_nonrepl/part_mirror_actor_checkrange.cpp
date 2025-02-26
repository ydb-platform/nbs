#include "part_mirror_actor.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/partition_common/actor_checkrange.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/xrange.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMirrorCheckRangeActor final: public TCheckRangeActor
{
public:
    TMirrorCheckRangeActor(
        const TActorId& partition,
        ui64 startIndex,
        ui64 blocksCount,
        TRequestInfoPtr&& requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    void SendReadBlocksRequest(const TActorContext& ctx) override;
};

////////////////////////////////////////////////////////////////////////////////

TMirrorCheckRangeActor::TMirrorCheckRangeActor(
    const TActorId& partition,
    ui64 startIndex,
    ui64 blocksCount,
    TRequestInfoPtr&& requestInfo)
    : TCheckRangeActor(partition, startIndex, blocksCount, std::move(requestInfo))
{}

void TMirrorCheckRangeActor::SendReadBlocksRequest(const TActorContext& ctx)
{
    const TString clientId{CheckRangeClientId};
    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();

    request->Record.SetStartIndex(StartIndex);
    request->Record.SetBlocksCount(BlocksCount);

    auto* headers = request->Record.MutableHeaders();

    headers->SetClientId(clientId);
    headers->SetIsBackgroundRequest(true);

    NCloud::Send(ctx, Partition, std::move(request));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandleCheckRange(
    const TEvService::TEvCheckRangeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto error = ValidateBlocksCount(
        msg->Record.GetBlocksCount(),
        Config->GetBytesPerStripe(),
        State.GetBlockSize(),
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

}   // namespace NCloud::NBlockStore::NStorage
