#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service.h>

#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NMonitoring;
using namespace NProto;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TVolumeStatActor final
    : public TActorBootstrapped<TVolumeStatActor>
{
private:
    const TActorId VolumeBalancerActorId;
    TVector<NProto::TVolumeBalancerDiskStats> Volumes;

public:
    TVolumeStatActor(
            TActorId volumeBalancerActorId,
            TVector<NProto::TVolumeBalancerDiskStats> volumes)
        : VolumeBalancerActorId(volumeBalancerActorId)
        , Volumes(std::move(volumes))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        auto request = std::make_unique<TEvStatsService::TEvGetVolumeStatsRequest>(std::move(Volumes));
        NCloud::Send(ctx, MakeStorageStatsServiceId(), std::move(request));

        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork);

    void HandleGetVolumeStatsResponse(
        const TEvStatsService::TEvGetVolumeStatsResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();
        auto response = std::make_unique<TEvService::TEvGetVolumeStatsResponse>(std::move(msg->VolumeStats));
        NCloud::Send(ctx, VolumeBalancerActorId, std::move(response));
        Die(ctx);
    }
};

////////////////////////////////////////////////////////////////////////////////


STFUNC(TVolumeStatActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvStatsService::TEvGetVolumeStatsResponse, HandleGetVolumeStatsResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}    // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleGetVolumeStats(
    const TEvService::TEvGetVolumeStatsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    TVector<NProto::TVolumeBalancerDiskStats> volumes;

    for (const auto& v: State.GetVolumes()) {
        if (v.second->IsLocallyMounted()) {
            NProto::TVolumeBalancerDiskStats stats;
            stats.SetDiskId(v.first);
            if (v.second->VolumeInfo) {
                stats.SetCloudId(v.second->VolumeInfo->GetCloudId());
                stats.SetFolderId(v.second->VolumeInfo->GetFolderId());
                stats.SetStorageMediaKind(
                    v.second->VolumeInfo->GetStorageMediaKind());
            }

            if (v.second->BindingType != NProto::BINDING_REMOTE) {
                stats.SetHost(FQDNHostName());
                stats.SetIsLocal(true);
            }
            stats.SetPreemptionSource(v.second->PreemptionSource);
            volumes.push_back(std::move(stats));
        }
    }

    NCloud::Register<TVolumeStatActor>(
        ctx,
        ev->Sender,
        std::move(volumes));
}

}   // namespace NCloud::NBlockStore::NStorage
