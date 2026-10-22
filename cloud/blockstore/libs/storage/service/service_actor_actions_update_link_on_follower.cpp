#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/string/builder.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto UpdateLinkTimeout = TDuration::Seconds(30);

class TUpdateLinkOnFollowerActionActor final
    : public TActorBootstrapped<TUpdateLinkOnFollowerActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;
    const TString CellId;

public:
    TUpdateLinkOnFollowerActionActor(TRequestInfoPtr requestInfo, TString input,
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
        const auto status =
            google::protobuf::util::JsonStringToMessage(Input, &record);
        if (!status.ok()) {
            ReplyAndDie(
                ctx,
                MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "Failed to parse input: " << status.ToString()));
            return;
        }

        if (!record.GetDiskId() || !record.GetLeaderDiskId()) {
            ReplyAndDie(ctx, MakeError(E_ARGUMENT,
                                       "DiskId and LeaderDiskId are required"));
            return;
        }

        switch (record.GetAction()) {
            case NProto::LINK_ACTION_CREATE:
            case NProto::LINK_ACTION_COMPLETED:
                if (!record.GetLinkUUID()) {
                    ReplyAndDie(ctx,
                                MakeError(E_ARGUMENT, "LinkUUID is required"));
                    return;
                }
                break;
            case NProto::LINK_ACTION_DESTROY:
                // The leader may already be gone, so its link UUID is unknown.
                break;
            default:
                ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Invalid link action"));
                return;
        }

        if (record.GetFollowerCellId() && record.GetFollowerCellId() != CellId)
        {
            ReplyAndDie(ctx,
                        MakeError(E_ARGUMENT,
                                  "FollowerCellId does not match this cell"));
            return;
        }

        // Headers in Input are not part of the action's contract.
        record.ClearHeaders();
        record.MutableHeaders()->SetExactDiskIdMatch(true);

        Become(&TThis::StateWork);
        NCloud::SendWithUndeliveryTracking(ctx, MakeVolumeProxyServiceId(),
                                           std::move(request));
        ctx.Schedule(UpdateLinkTimeout, new TEvents::TEvWakeup());
    }

private:
    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error,
                     TString output = {})
    {
        auto response =
            std::make_unique<TEvService::TEvExecuteActionResponse>(error);
        response->Record.SetOutput(std::move(output));

        LWTRACK(ResponseSent_Service, RequestInfo->CallContext->LWOrbit,
                "ExecuteAction_updatelinkonfollower",
                RequestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        Die(ctx);
    }

    void HandleResponse(
        const TEvVolume::TEvUpdateLinkOnFollowerResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        TString output;
        const auto status =
            google::protobuf::util::MessageToJsonString(msg->Record, &output);
        if (!status.ok()) {
            ReplyAndDie(ctx, MakeError(E_FAIL, "Failed to serialize response"));
            return;
        }
        ReplyAndDie(ctx, msg->GetError(), std::move(output));
    }

    void HandleUndelivery(
        const TEvVolume::TEvUpdateLinkOnFollowerRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        ReplyAndDie(ctx, MakeError(E_REJECTED, "Volume proxy is unavailable"));
    }

    void HandleTimeout(const TActorContext& ctx)
    {
        ReplyAndDie(ctx,
                    MakeError(E_TIMEOUT, "UpdateLinkOnFollower timed out"));
    }

    void HandlePoisonPill(const TActorContext& ctx)
    {
        ReplyAndDie(ctx, MakeError(E_REJECTED, "UpdateLinkOnFollower stopped"));
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvVolume::TEvUpdateLinkOnFollowerResponse, HandleResponse);
            HFunc(TEvVolume::TEvUpdateLinkOnFollowerRequest, HandleUndelivery);
            CFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
            CFunc(TEvents::TEvPoisonPill::EventType, HandlePoisonPill);
            default:
                HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE,
                                      __PRETTY_FUNCTION__);
                break;
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateUpdateLinkOnFollowerActionActor(
    TRequestInfoPtr requestInfo, TString input)
{
    return {std::make_unique<TUpdateLinkOnFollowerActionActor>(
        std::move(requestInfo), std::move(input), CellId)};
}

}   // namespace NCloud::NBlockStore::NStorage
