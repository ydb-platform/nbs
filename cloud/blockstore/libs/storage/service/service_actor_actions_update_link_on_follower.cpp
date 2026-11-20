#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration UpdateLinkOnFollowerTimeout = TDuration::Seconds(30);

////////////////////////////////////////////////////////////////////////////////

class TUpdateLinkOnFollowerActionActor final
    : public TActorBootstrapped<TUpdateLinkOnFollowerActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;
    const TString CellId;

public:
    TUpdateLinkOnFollowerActionActor(
        TRequestInfoPtr requestInfo,
        TString input,
        TString cellId)
        : RequestInfo(std::move(requestInfo))
        , Input(std::move(input))
        , CellId(std::move(cellId))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        auto request =
            std::make_unique<TEvVolume::TEvUpdateLinkOnFollowerRequest>(
                RequestInfo->CallContext);
        auto& record = request->Record;
        if (!google::protobuf::util::JsonStringToMessage(Input, &record).ok()) {
            ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
            return;
        }

        if (!record.GetDiskId() || !record.GetLinkUUID() ||
            !record.GetLeaderDiskId() || !record.GetLeaderCellId() ||
            !record.GetFollowerCellId())
        {
            ReplyAndDie(
                ctx,
                MakeError(
                    E_ARGUMENT,
                    "DiskId, LinkUUID, LeaderDiskId, LeaderCellId and "
                    "FollowerCellId should be supplied"));
            return;
        }

        if (record.GetFollowerCellId() != CellId) {
            ReplyAndDie(
                ctx,
                MakeError(
                    E_ARGUMENT,
                    "FollowerCellId does not match this cell"));
            return;
        }

        if (record.GetLeaderCellId() == record.GetFollowerCellId() &&
            record.GetLeaderDiskId() == record.GetDiskId())
        {
            ReplyAndDie(
                ctx,
                MakeError(
                    E_ARGUMENT,
                    "Leader and follower should be different"));
            return;
        }

        if (!NProto::ELinkAction_IsValid(record.GetAction())) {
            ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Unknown link action"));
            return;
        }

        // Input is untrusted protobuf JSON, including its internal headers.
        record.ClearHeaders();
        auto* headers = record.MutableHeaders();
        headers->SetExactDiskIdMatch(true);
        headers->SetRequestId(RequestInfo->CallContext->RequestId);

        Become(&TThis::StateWork);
        ctx.Schedule(UpdateLinkOnFollowerTimeout, new TEvents::TEvWakeup());
        NCloud::SendWithUndeliveryTracking(
            ctx,
            MakeVolumeProxyServiceId(),
            std::move(request));
    }

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error)
    {
        auto response =
            std::make_unique<TEvService::TEvExecuteActionResponse>(
                std::move(error));
        LWTRACK(
            ResponseSent_Service,
            RequestInfo->CallContext->LWOrbit,
            "ExecuteAction_updatelinkonfollower",
            RequestInfo->CallContext->RequestId);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        Die(ctx);
    }

    void HandleResponse(
        const TEvVolume::TEvUpdateLinkOnFollowerResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        ReplyAndDie(ctx, ev->Get()->GetError());
    }

    void HandleUndelivery(const TActorContext& ctx)
    {
        ReplyAndDie(
            ctx,
            MakeError(
                E_REJECTED,
                "UpdateLinkOnFollower could not reach VolumeProxy"));
    }

    void HandleTimeout(const TActorContext& ctx)
    {
        // A timeout does not imply that the follower did not apply the update.
        ReplyAndDie(
            ctx,
            MakeError(E_TIMEOUT, "UpdateLinkOnFollower timed out"));
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvVolume::TEvUpdateLinkOnFollowerResponse, HandleResponse);
            CFunc(TEvVolume::EvUpdateLinkOnFollowerRequest, HandleUndelivery);
            CFunc(TEvents::TEvWakeup::EventType, HandleTimeout);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::SERVICE,
                    __PRETTY_FUNCTION__);
                break;
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateUpdateLinkOnFollowerActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TUpdateLinkOnFollowerActionActor>(
        std::move(requestInfo),
        std::move(input),
        CellId)};
}

}   // namespace NCloud::NBlockStore::NStorage
