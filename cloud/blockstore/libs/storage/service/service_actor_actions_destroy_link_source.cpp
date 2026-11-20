#include "service_actor.h"
#include "service_destroy_volume_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

class TDestroyLinkSourceActionActor final
    : public TActorBootstrapped<TDestroyLinkSourceActionActor>
{
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const TString Input;
    const TString CellId;
    NProto::TGetLinkStatusRequest Link;
    ui64 PathId = 0;
    ui64 PathVersion = 0;
    ui64 VolumeTabletId = 0;
    ui64 FillGeneration = 0;

public:
    TDestroyLinkSourceActionActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString input,
        TString cellId)
        : RequestInfo(std::move(requestInfo))
        , Config(std::move(config))
        , Input(std::move(input))
        , CellId(std::move(cellId))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        if (!google::protobuf::util::JsonStringToMessage(Input, &Link).ok() ||
            !Link.GetLinkUUID() || !Link.GetLeaderDiskId() ||
            !Link.GetFollowerDiskId() || !Link.GetFollowerCellId() ||
            Link.GetLeaderCellId() != CellId ||
            Link.GetLeaderCellId() == Link.GetFollowerCellId())
        {
            Reply(ctx, MakeError(E_ARGUMENT, "Invalid source cleanup identity"));
            return;
        }
        Link.ClearHeaders();
        Link.MutableHeaders()->SetExactDiskIdMatch(true);
        Link.MutableHeaders()->SetRequestId(RequestInfo->CallContext->RequestId);
        Link.SetDiskId(Link.GetLeaderDiskId());
        Link.SetIncludePendingCreation(false);
        Become(&TThis::StateWork);
        ctx.Schedule(Config->GetDestroyVolumeTimeout(), new TEvents::TEvWakeup());
        NCloud::SendWithUndeliveryTracking(ctx, MakeSSProxyServiceId(),
            std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(
                Link.GetLeaderDiskId(), true));
    }

private:
    void Reply(const TActorContext& ctx, NProto::TError error)
    {
        NCloud::Reply(ctx, *RequestInfo,
            std::make_unique<TEvService::TEvExecuteActionResponse>(std::move(error)));
        Die(ctx);
    }

    void HandleDescribe(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* message = ev->Get();
        if (IsNotFoundSchemeShardError(message->GetError())) {
            Reply(ctx, MakeError(S_ALREADY, "Source volume is already absent"));
            return;
        }
        if (HasError(message->GetError())) {
            Reply(ctx, message->GetError());
            return;
        }
        const auto& path = message->PathDescription;
        const auto& description = path.GetBlockStoreVolumeDescription();
        PathId = path.GetSelf().GetPathId();
        PathVersion = path.GetSelf().GetPathVersion();
        VolumeTabletId = description.GetVolumeTabletId();
        FillGeneration = description.GetVolumeConfig().GetFillGeneration();
        if (!PathId || !VolumeTabletId ||
            description.GetVolumeConfig().GetDiskId() != Link.GetLeaderDiskId())
        {
            Reply(ctx, MakeError(E_REJECTED, "Source identity is not authoritative"));
            return;
        }
        auto request = std::make_unique<TEvVolume::TEvGetLinkStatusRequest>(
            RequestInfo->CallContext);
        request->Record = Link;
        NCloud::SendWithUndeliveryTracking(
            ctx, MakeVolumeProxyServiceId(), std::move(request));
    }

    void HandleStatus(
        const TEvVolume::TEvGetLinkStatusResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* message = ev->Get();
        if (HasError(message->GetError())) {
            Reply(ctx, message->GetError());
            return;
        }
        const auto& status = message->Record;
        if (status.GetVolumeTabletId() != VolumeTabletId ||
            status.GetLinkUUID() != Link.GetLinkUUID() ||
            status.GetLeaderCellId() != CellId ||
            status.GetFollowerCellId() != Link.GetFollowerCellId())
        {
            Reply(ctx, MakeError(E_PRECONDITION_FAILED,
                "Source was replaced or does not own the expected link"));
            return;
        }
        if (status.GetStatus() != NProto::LINK_STATUS_LEADERSHIP_TRANSFERRED) {
            Reply(ctx, MakeError(E_REJECTED, "Source has not committed handover"));
            return;
        }
        // Bind the schema DROP to the same object whose tablet evaluated UUID.
        // The guard is passed through the existing deletion/device cleanup path.
        NCloud::Register(ctx, CreateConditionalDestroyVolumeActor(
            CreateRequestInfo(SelfId(), 0, RequestInfo->CallContext),
            Config, Link.GetLeaderDiskId(), FillGeneration, PathId, PathVersion));
    }

    void HandleDestroyed(
        const TEvService::TEvDestroyVolumeResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        Reply(ctx, ev->Get()->GetError());
    }

    void HandleUndelivery(const TActorContext& ctx)
    {
        Reply(ctx, MakeError(E_REJECTED, "Source cleanup request was not delivered"));
    }

    void HandleTimeout(const TActorContext& ctx)
    {
        Reply(ctx, MakeError(E_TIMEOUT, "Source cleanup timed out"));
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvSSProxy::TEvDescribeVolumeResponse, HandleDescribe);
            HFunc(TEvVolume::TEvGetLinkStatusResponse, HandleStatus);
            HFunc(TEvService::TEvDestroyVolumeResponse, HandleDestroyed);
            CFunc(TEvSSProxy::EvDescribeVolumeRequest, HandleUndelivery);
            CFunc(TEvVolume::EvGetLinkStatusRequest, HandleUndelivery);
            CFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
            default:
                HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE,
                    __PRETTY_FUNCTION__);
                break;
        }
    }
};

}   // namespace

TResultOrError<IActorPtr> TServiceActor::CreateDestroyLinkSourceActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TDestroyLinkSourceActionActor>(
        std::move(requestInfo), Config, std::move(input), CellId)};
}

}   // namespace NCloud::NBlockStore::NStorage
