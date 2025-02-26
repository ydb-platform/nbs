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
        const TActorId& partition,
        NProto::TCheckRangeRequest&& request,
        TRequestInfoPtr&& requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    void SendReadBlocksRequest(const TActorContext& ctx) override;
};

////////////////////////////////////////////////////////////////////////////////

TNonreplCheckRangeActor::TNonreplCheckRangeActor(
    const TActorId& partition,
    NProto::TCheckRangeRequest&& request,
    TRequestInfoPtr&& requestInfo)
    : TCheckRangeActor(partition, std::move(request), std::move(requestInfo))
{}

void TNonreplCheckRangeActor::SendReadBlocksRequest(const TActorContext& ctx)
{
    const TString clientId{CheckRangeClientId};
    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();

    request->Record.SetStartIndex(Request.GetStartIndex());
    request->Record.SetBlocksCount(Request.GetBlocksCount());

    auto* headers = request->Record.MutableHeaders();
    headers->SetClientId(clientId);
    headers->SetIsBackgroundRequest(true);

    NCloud::Send(ctx, Partition, std::move(request));
}

}   // namespace

//////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::HandleCheckRange(
    const TEvService::TEvCheckRangeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto& record = ev->Get()->Record;

    auto error = ValidateBlocksCount(
        record.GetBlocksCount(),
        Config->GetBytesPerStripe(),
        PartConfig->GetBlockSize(),
        Config->GetCheckRangeMaxRangeSize());

    if (HasError(error)) {
        auto response =
            std::make_unique<TEvService::TEvCheckRangeResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto actorId = NCloud::Register<TNonreplCheckRangeActor>(
        ctx,
        SelfId(),
        std::move(record),
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext));

    RequestsInProgress.AddReadRequest(actorId);
}

}   // namespace NCloud::NBlockStore::NStorage
