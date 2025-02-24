#include "part_nonrepl_actor.h"

#include "cloud/blockstore/libs/storage/disk_agent/model/public.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/guid.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/xrange.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCheckRangeActor final: public TActorBootstrapped<TCheckRangeActor>
{
private:
    const TActorId Tablet;
    const ui64 StartIndex;
    const ui64 BlocksCount;
    const TRequestInfoPtr RequestInfo;

public:
    TCheckRangeActor(
        const TActorId& tablet,
        ui64 blockId,
        ui64 blocksCount,
        TRequestInfoPtr&& requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& status,
        const NProto::TError& error = {});

    void HandleReadBlocksResponse(
        const TEvService::TEvReadBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void SendReadBlocksRequest(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCheckRangeActor::TCheckRangeActor(
    const TActorId& tablet,
    ui64 startIndex,
    ui64 blocksCount,
    TRequestInfoPtr&& requestInfo)
    : Tablet(tablet)
    , StartIndex(startIndex)
    , BlocksCount(blocksCount)
    , RequestInfo(std::move(requestInfo))
{}

void TCheckRangeActor::Bootstrap(const TActorContext& ctx)
{
    SendReadBlocksRequest(ctx);
    Become(&TThis::StateWork);
}

void TCheckRangeActor::SendReadBlocksRequest(const TActorContext& ctx)
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

void TCheckRangeActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& status,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvCheckRangeResponse>(std::move(error));
    response->Record.MutableStatus()->CopyFrom(status);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCheckRangeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvService::TEvReadBlocksResponse, HandleReadBlocksResponse);
        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

void TCheckRangeActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    SendReadBlocksRequest(ctx);
}

void TCheckRangeActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "tablet is shutting down");

    ReplyAndDie(ctx, MakeError(S_OK), error);
}

void TCheckRangeActor::HandleReadBlocksResponse(
    const TEvService::TEvReadBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto status = MakeError(S_OK);

    const auto& error = msg->Record.GetError();
    if (HasError(error)) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::PARTITION,
            "reading error has occurred: " << FormatError(error));
        status = error;
    }

    ReplyAndDie(ctx, status);
}

}   // namespace

//////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::HandleCheckRange(
    const TEvService::TEvCheckRangeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ui64 blocksPerStripe =
        Config->GetBytesPerStripe() / PartConfig->GetBlockSize();
    const ui64 maxBlocksPerRequest =
        Min<ui64>(blocksPerStripe, Config->GetCheckRangeMaxRangeSize() / PartConfig->GetBlockSize());

    if (msg->Record.GetBlocksCount() > maxBlocksPerRequest) {
        auto err = MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Too many blocks requested: "
                             << std::to_string(msg->Record.GetBlocksCount())
                             << " Max blocks per request : "
                             << std::to_string(maxBlocksPerRequest));

        auto response =
            std::make_unique<TEvService::TEvCheckRangeResponse>(std::move(err));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto actorId = NCloud::Register<TCheckRangeActor>(
        ctx,
        SelfId(),
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount(),
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext));

    RequestsInProgress.AddReadRequest(actorId);
}

}   // namespace NCloud::NBlockStore::NStorage
