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

class TCleanupTabletHistoryActor final: public TActorBootstrapped<TCleanupTabletHistoryActor>
{
private:
    const TActorId Tablet;
    const NKikimr::TTabletStorageInfoPtr& Storage;
    const TEvService::TEvCheckRangeRequest::TPtr Ev;

public:
    TCleanupTabletHistory(
        const TActorId& tablet,
        const NKikimr::TTabletStorageInfoPtr& storage,
        TEvService::TEvCheckRangeRequest::TPtr&& ev);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCheckRangeActor::TCheckRangeActor(
    const TActorId& tablet,
    const NKikimr::TTabletStorageInfoPtr& storage,
    TEvService::TEvCheckRangeRequest::TPtr&& ev)
    : Tablet(tablet)
    , Storage(storage)
    , Ev(std::move(ev))
{}

void TCheckRangeActor::Bootstrap(const TActorContext& ctx)
{
    //Do Work
    LOG_ERROR(ctx, TBlockStoreComponents::PARTITION_WORKER, "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! AAAAAAAAAAAAAA");
    Become(&TThis::StateWork);
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
        HFunc(TEvService::TEvReadBlocksResponse, HandleReadBlocksResponse);
        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

void TCheckRangeActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "tablet is shutting down");

    ReplyAndDie(ctx, error);
}

}   // namespace

//////////////////////////////////////////////////////////////////

void NPartition::TPartitionActor::HandleCheckRange(
    const TEvService::TEvCheckRangeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto actorId = NCloud::Register<TCleanupTabletHistoryActor>(
        ctx,
        SelfId(),
        storage,
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext));

    Actors.Insert(actorId);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
