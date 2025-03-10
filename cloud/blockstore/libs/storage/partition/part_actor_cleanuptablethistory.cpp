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

class TCutTabletHistoryActor final
    : public TActorBootstrapped<TCutTabletHistoryActor>
{
private:
    const TActorId Tablet;
    const NKikimr::TTabletStorageInfoPtr TabletInfo;
    const TRequestInfoPtr RequestInfo;
    std::unordered_map<ui32, ui32> cutFromGeneration;

public:
    TCutTabletHistoryActor(
        const TActorId& tablet,
        NKikimr::TTabletStorageInfoPtr tabletInfo,
        TRequestInfoPtr&& requestInfo);

    void Bootstrap(const TActorContext& ctx);

    ui32 CountToBeDeleted(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error,
        const ui32 result = 0);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCutTabletHistoryActor::TCutTabletHistoryActor(
    const TActorId& tablet,
    NKikimr::TTabletStorageInfoPtr tabletInfo,
    TRequestInfoPtr&& requestInfo)
    : Tablet(tablet)
    , TabletInfo(std::move(tabletInfo))
    , RequestInfo(std::move(requestInfo))
{}

void TCutTabletHistoryActor::Bootstrap(const TActorContext& ctx)
{
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Bootstraped");
    auto result = CountToBeDeleted(ctx);
    LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION_WORKER, "" << result);
    ReplyAndDie(ctx, {}, result);

    Become(&TThis::StateWork);
}

ui32 TCutTabletHistoryActor::CountToBeDeleted(const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    TVector<std::unordered_map<ui32, ui32>> channelGroupMaxGeneration;

    std::unordered_set<ui32> groups;
    if (!TabletInfo->Channels || TabletInfo->Channels.empty()) {
        return 0;
    }

    const auto chanels = TabletInfo->Channels;
    ui32 total = 0;

    channelGroupMaxGeneration.resize(chanels.size());
    for (const auto& chanel: chanels) {
        auto history = TabletInfo->ChannelInfo(chanel.Channel)->History;
        total += history.size();

        for (ui32 i = 0; i < history.size(); ++i) {
            const auto& historyRecord = history[i];

            channelGroupMaxGeneration[i][historyRecord.GroupID] = Max<ui32>(
                channelGroupMaxGeneration[i][historyRecord.GroupID],
                historyRecord.FromGeneration);

            groups.insert(historyRecord.GroupID);
        }
    }

    ui32 groupsRemain = 0;
    for (auto group: groups) {
        ui32 minGeneration = std::numeric_limits<ui32>::max();
        bool hasGeneration = false;

        for (const auto& groupMap: channelGroupMaxGeneration) {
            auto it = groupMap.find(group);
            if (it != groupMap.end()) {
                minGeneration = std::min(minGeneration, it->second);
                hasGeneration = true;
                ++groupsRemain;
            }
        }

        if (hasGeneration) {
            cutFromGeneration[group] = minGeneration;
        }
    }
    return total - groupsRemain;
}

void TCutTabletHistoryActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error,
    const ui32 result)
{
    auto response = std::make_unique<TEvVolume::TEvCutTabletHistoryResponse>(
        std::move(error));
    response.get()->Record.SetToBeDelited(result);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCutTabletHistoryActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

void TCutTabletHistoryActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "tablet is shutting down");

    ReplyAndDie(ctx, error);
}

}   // namespace

//////////////////////////////////////////////////////////////////

void NPartition::TPartitionActor::HandleCutTabletHistory(
    const TEvVolume::TEvCutTabletHistoryRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // const auto* msg = ev->Get();

    const auto actorId = NCloud::Register<TCutTabletHistoryActor>(
        ctx,
        SelfId(),
        Info(),
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext));

    Actors.Insert(actorId);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
