#include "hive_proxy_actor.h"

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void THiveProxyActor::HandleGetStorageInfo(
    const TEvHiveProxy::TEvGetStorageInfoRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ui64 tabletId = msg->TabletId;
    ui64 hive = GetHive(ctx, tabletId);

    auto& requests = HiveStates[hive].GetInfoRequests[tabletId];
    requests.emplace_back(ev->Sender, ev->Cookie);

    if (requests.size() == 1) {
        // Send request to hive on the first incoming request
        SendGetTabletStorageInfoRequest(ctx, hive, tabletId);
    }
}

void THiveProxyActor::HandleGetTabletStorageInfoRegistered(
    const TEvHive::TEvGetTabletStorageInfoRegistered::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
}

void THiveProxyActor::HandleGetTabletStorageInfoResult(
    const TEvHive::TEvGetTabletStorageInfoResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ui64 tabletId = msg->Record.GetTabletID();
    ui64 hive = GetHive(ctx, tabletId);

    NProto::TError error;
    TTabletStorageInfoPtr storageInfo;

    if (msg->Record.GetStatus() != NKikimrProto::OK) {
        error =
            MakeKikimrError(msg->Record.GetStatus(), "GetStorageInfo failed");
    } else {
        storageInfo = TabletStorageInfoFromProto(msg->Record.GetInfo());
    }

    auto& states = HiveStates[hive];
    auto& requests = states.GetInfoRequests[tabletId];
    while (!requests.empty()) {
        auto response =
            std::make_unique<TEvHiveProxy::TEvGetStorageInfoResponse>(
                error,
                storageInfo);

        NCloud::Reply(ctx, requests.front(), std::move(response));
        requests.pop_front();
    }
    states.GetInfoRequests.erase(tabletId);
}

}   // namespace NCloud::NStorage
