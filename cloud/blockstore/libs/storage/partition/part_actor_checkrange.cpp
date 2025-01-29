#include "part_actor.h"

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
    const ui64 FirstBlockOffset;
    const ui64 BlocksCount;
    const TDuration Timeout;
    const TActorId Sender;
    const TEvVolume::TEvCheckRangeRequest::TPtr Ev;

public:
    TCheckRangeActor(
        const TActorId& tablet,
        ui64 blockId,
        ui64 blocksCount,
        TDuration timeout,
        TEvVolume::TEvCheckRangeRequest::TPtr&& ev);

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
    ui64 blockOffset,
    ui64 blocksCount,
    TDuration timeout,
    TEvVolume::TEvCheckRangeRequest::TPtr&& ev)
    : Tablet(tablet)
    , FirstBlockOffset(blockOffset)
    , BlocksCount(blocksCount)
    , Timeout(std::move(timeout))
    , Ev(std::move(ev))
{}

void TCheckRangeActor::Bootstrap(const TActorContext& ctx)
{
    SendReadBlocksRequest(ctx);
    Become(&TThis::StateWork);
}

void TCheckRangeActor::SendReadBlocksRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();

    request->Record.SetStartIndex(FirstBlockOffset);
    request->Record.SetBlocksCount(BlocksCount);

    auto* headers = request->Record.MutableHeaders();

    headers->SetIsBackgroundRequest(true);
    NCloud::Send(ctx, Tablet, std::move(request));
}

void TCheckRangeActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvVolume::TEvCheckRangeResponse>(std::move(error));

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

    if (HasError(msg->Record.GetError())) {
        auto errorMessage = msg->Record.GetError().GetMessage();
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "reading error has occurred: " + errorMessage + "   message   " +
                msg->Record.GetError().message());
    }

    ReplyAndDie(ctx, msg->Record.GetError());
}

}   // namespace

}   // namespace NCloud::NBlockStore::NStorage::NPartition

namespace NCloud::NBlockStore::NStorage::NPartition {
NActors::IActorPtr TPartitionActor::CreateCheckRangeActor(
    NActors::TActorId tablet,
    ui64 blockOffset,
    ui64 blocksCount,
    TDuration retryTimeout,
    TEvVolume::TEvCheckRangeRequest::TPtr ev)
{
    return std::make_unique<NPartition::TCheckRangeActor>(
        std::move(tablet),
        blockOffset,
        blocksCount,
        retryTimeout,
        std::move(ev));
}

////////////////////////////////////////////////////////////////////////////////

void NPartition::TPartitionActor::HandleCheckRange(
    const TEvVolume::TEvCheckRangeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (msg->Record.GetBlockCount() > Config->GetBytesPerStripe()) {
        auto err = MakeError(
            E_ARGUMENT,
            "Too many blocks requested: " +
                std::to_string(msg->Record.GetBlockCount()) +
                " Max blocks per request : " +
                std::to_string(Config->GetBytesPerStripe()));
        auto response =
            std::make_unique<TEvVolume::TEvCheckRangeResponse>(std::move(err));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto actorId = NCloud::Register(
        ctx,
        CreateCheckRangeActor(
            SelfId(),
            msg->Record.GetBlockIdx(),
            msg->Record.GetBlockCount(),
            Config->GetCompactionRetryTimeout(),
            std::move(ev)));

    Actors.Insert(actorId);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
