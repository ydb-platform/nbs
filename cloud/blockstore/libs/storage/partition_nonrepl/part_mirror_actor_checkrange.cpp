#include "part_mirror_actor.h"

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

namespace NCloud::NBlockStore::NStorage::NPartition {

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
    const TActorId Sender;
    const TEvService::TEvCheckRangeRequest::TPtr Ev;

public:
    TCheckRangeActor(
        const TActorId& tablet,
        ui64 startIndex,
        ui64 blocksCount,
        TEvService::TEvCheckRangeRequest::TPtr&& ev);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
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
    TEvService::TEvCheckRangeRequest::TPtr&& ev)
    : Tablet(tablet)
    , StartIndex(startIndex)
    , BlocksCount(blocksCount)
    , Ev(std::move(ev))
{}

void TCheckRangeActor::Bootstrap(const TActorContext& ctx)
{
    SendReadBlocksRequest(ctx);
    Become(&TThis::StateWork);
}

void TCheckRangeActor::SendReadBlocksRequest(const TActorContext& ctx)
{
    const TString clientId = TString(CheckRangeClientId);
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
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvCheckRangeResponse>(std::move(error));

    NCloud::Reply(ctx, *Ev, std::move(response));

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

    ReplyAndDie(ctx, error);
}

void TCheckRangeActor::HandleReadBlocksResponse(
    const TEvService::TEvReadBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto error = MakeError(S_OK);

    if (HasError(msg->Record.GetError())) {
        auto errorMessage = msg->Record.GetError().GetMessage();
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "reading error has occurred: " + errorMessage + "   message   " +
                msg->Record.GetError().GetMessage());
        auto errorCode =
            msg->Record.GetError().code() == E_ARGUMENT ? E_ARGUMENT : E_IO;
        error = MakeError(errorCode, msg->Record.GetError().GetMessage());
    }

    ReplyAndDie(ctx, error);
}

}   // namespace

}   // namespace NCloud::NBlockStore::NStorage::NPartition

namespace NCloud::NBlockStore::NStorage {

NActors::IActorPtr TMirrorPartitionActor::CreateCheckRangeActor(
    NActors::TActorId tablet,
    ui64 startIndex,
    ui64 blocksCount,
    TEvService::TEvCheckRangeRequest::TPtr ev)
{
    return std::make_unique<NPartition::TCheckRangeActor>(
        std::move(tablet),
        startIndex,
        blocksCount,
        std::move(ev));
}

void TMirrorPartitionActor::HandleCheckRange(
    const TEvService::TEvCheckRangeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui64 maxBlocksPerRequest =
        Config->GetBytesPerStripe() / State.GetBlockSize();

    if (msg->Record.GetBlocksCount() > maxBlocksPerRequest) {
        auto err = MakeError(
            E_ARGUMENT,
            "Too many blocks requested: " +
                std::to_string(msg->Record.GetBlocksCount()) +
                " Max blocks per request : " +
                std::to_string(maxBlocksPerRequest));
        auto response =
            std::make_unique<TEvService::TEvCheckRangeResponse>(std::move(err));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto blockRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount());

    const auto requestIdentityKey = ev->Cookie;
    RequestsInProgress.AddReadRequest(requestIdentityKey, blockRange);

    NCloud::Register(
        ctx,
        CreateCheckRangeActor(
            SelfId(),
            msg->Record.GetStartIndex(),
            msg->Record.GetBlocksCount(),
            std::move(ev)));
}

}   // namespace NCloud::NBlockStore::NStorage
