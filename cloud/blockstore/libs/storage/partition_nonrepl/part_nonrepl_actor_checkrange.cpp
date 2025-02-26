#include "part_nonrepl_actor.h"

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

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TNonreplCheckRangeActor final: public TCheckRangeActor
{
public:
    TNonreplCheckRangeActor(
        const TActorId& tablet,
        ui64 startIndex,
        ui64 blocksCount,
        TRequestInfoPtr&& requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    void SendReadBlocksRequest(const TActorContext& ctx) override;
};

////////////////////////////////////////////////////////////////////////////////

TNonreplCheckRangeActor::TNonreplCheckRangeActor(
    const TActorId& tablet,
    ui64 startIndex,
    ui64 blocksCount,
    TRequestInfoPtr&& requestInfo)
    : TCheckRangeActor(tablet, startIndex, blocksCount, std::move(requestInfo))
{}

void TNonreplCheckRangeActor::SendReadBlocksRequest(const TActorContext& ctx)
{
    const TString clientId{CheckRangeClientId};
    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();

    request->Record.SetStartIndex(StartIndex);
    request->Record.SetBlocksCount(BlocksCount);

    auto* headers = request->Record.MutableHeaders();
    headers->SetClientId(clientId);
    headers->SetIsBackgroundRequest(true);

    NCloud::Send(ctx, Tablet, std::move(request));
}

}   // namespace

//////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::HandleCheckRange(
    const TEvService::TEvCheckRangeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (auto error = ValidateBlocksCount(
            msg->Record.GetBlocksCount(),
            Config->GetBytesPerStripe(),
            PartConfig->GetBlockSize(),
            Config->GetCheckRangeMaxRangeSize()))
    {
        auto response =
            std::make_unique<TEvService::TEvCheckRangeResponse>(*error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto actorId = NCloud::Register<TNonreplCheckRangeActor>(
        ctx,
        SelfId(),
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount(),
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext));

    RequestsInProgress.AddReadRequest(actorId);
}

}   // namespace NCloud::NBlockStore::NStorage
