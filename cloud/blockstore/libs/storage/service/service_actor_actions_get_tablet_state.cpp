#include "service_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/hive_proxy/tablet_boot_info.h>

#include <contrib/ydb/core/base/tablet_pipe.h>
#include <contrib/ydb/core/node_whiteboard/node_whiteboard.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NNodeWhiteboard;
using namespace NCloud::NStorage;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultTimeoutMs = 5'000;
constexpr ui32 MaxTimeoutMs = 30'000;

using TResponse = NPrivateProto::TGetTabletStateResponse;
using TWhiteboardInfo = NKikimrWhiteboard::TTabletStateInfo;

TResponse::ELocalState GetLocalState(const TWhiteboardInfo& info)
{
    if (!info.HasState()) {
        return TResponse::LOCAL_UNKNOWN;
    }

    switch (info.GetState()) {
        case TWhiteboardInfo::Created:
        case TWhiteboardInfo::ResolveStateStorage:
        case TWhiteboardInfo::Candidate:
        case TWhiteboardInfo::BlockBlobStorage:
        case TWhiteboardInfo::RebuildGraph:
        case TWhiteboardInfo::WriteZeroEntry:
        case TWhiteboardInfo::Restored:
        case TWhiteboardInfo::Discover:
        case TWhiteboardInfo::Lock:
        case TWhiteboardInfo::ResolveLeader:
            return TResponse::LOCAL_STARTING;
        case TWhiteboardInfo::Active:
            return info.GetLeader() ? TResponse::LOCAL_ACTIVE
                                    : TResponse::LOCAL_UNKNOWN;
        case TWhiteboardInfo::Dead:
        case TWhiteboardInfo::Deleted:
        case TWhiteboardInfo::Stopped:
            return TResponse::LOCAL_STOPPED;
        default:
            return TResponse::LOCAL_UNKNOWN;
    }
}

class TGetTabletStateActor final
    : public TActorBootstrapped<TGetTabletStateActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TDuration Timeout;

    TResponse Result;
    TActorId PipeClient;
    bool PipeDone = false;
    bool WhiteboardDone = false;
    bool BackupDone = false;

public:
    TGetTabletStateActor(
        TRequestInfoPtr requestInfo,
        const NPrivateProto::TGetTabletStateRequest& request)
        : RequestInfo(std::move(requestInfo))
        , Timeout(
              TDuration::MilliSeconds(
                  request.GetTimeoutMs() ? request.GetTimeoutMs()
                                         : DefaultTimeoutMs))
    {
        Result.SetTabletId(request.GetTabletId());
        Result.SetMessage("Leader probe timed out; tablet state is unknown");
        Result.SetLocalStateMessage("Local whiteboard request timed out");
        Result.SetBootInfoMessage("Local backup request timed out");
    }

    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TThis::StateWork);
        ctx.Schedule(Timeout, new TEvents::TEvWakeup());
        Result.SetLocalNodeId(ctx.SelfID.NodeId());

        // Resolve and connect only. CheckAliveness may contact Hive on failure.
        // Do not send any application request that could start child tablets.
        NTabletPipe::TClientConfig config;
        // NBS tablets accept TEvConnect through their system actor. The
        // user tablet does not implement the pipe connection handshake.
        config.ConnectToUserTablet = false;
        config.AllowFollower = false;
        config.CheckAliveness = false;
        // The resolver may still cache the address from before a tablet
        // restart. Retry once after the pipe invalidates that address.
        config.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        config.RetryPolicy.RetryLimitCount = 1;
        PipeClient = ctx.Register(
            NTabletPipe::CreateClient(SelfId(), Result.GetTabletId(), config));

        auto whiteboard =
            std::make_unique<TEvWhiteboard::TEvTabletStateRequest>();
        whiteboard->Record.AddFilterTabletId(Result.GetTabletId());
        ctx.Send(
            MakeNodeWhiteboardServiceId(ctx.SelfID.NodeId()),
            whiteboard.release(),
            IEventHandle::FlagTrackDelivery);

        // This request reads the loaded local backup in both normal and
        // fallback modes. GetStorageInfo would contact Hive in normal mode.
        ctx.Send(
            MakeHiveProxyServiceId(),
            new TEvHiveProxy::TEvGetTabletBootInfosRequest(
                Result.GetTabletId()),
            IEventHandle::FlagTrackDelivery);
    }

private:
    void ReplyAndDie(const TActorContext& ctx)
    {
        if (PipeClient) {
            NTabletPipe::CloseClient(ctx, PipeClient);
            PipeClient = {};
        }

        auto response =
            std::make_unique<TEvService::TEvExecuteActionResponse>();
        google::protobuf::util::JsonPrintOptions options;
        options.always_print_primitive_fields = true;
        const auto status = google::protobuf::util::MessageToJsonString(
            Result,
            response->Record.MutableOutput(),
            options);
        if (!status.ok()) {
            *response->Record.MutableError() =
                MakeError(E_FAIL, "Failed to serialize tablet state");
        }

        LWTRACK(
            ResponseSent_Service,
            RequestInfo->CallContext->LWOrbit,
            "ExecuteAction_gettabletstate",
            RequestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        Die(ctx);
    }

    void MaybeReply(const TActorContext& ctx)
    {
        if (PipeDone && WhiteboardDone && BackupDone) {
            ReplyAndDie(ctx);
        }
    }

    void HandleClientConnected(
        const TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto& msg = *ev->Get();
        if (msg.ClientId != PipeClient || msg.TabletId != Result.GetTabletId())
        {
            return;
        }

        PipeDone = true;
        if (msg.Status == NKikimrProto::OK && msg.Leader && msg.ServerId) {
            Result.SetState(TResponse::RUNNING);
            Result.SetLeaderNodeId(msg.ServerId.NodeId());
            Result.SetLeaderGeneration(msg.Generation);
            Result.SetMessage("Connected to the leader tablet");
        } else {
            Result.SetMessage(
                "Could not connect to the leader; tablet state is unknown");
        }
        MaybeReply(ctx);
    }

    void HandleClientDestroyed(
        const TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const TActorContext& ctx)
    {
        if (ev->Get()->ClientId != PipeClient) {
            return;
        }

        PipeDone = true;
        Result.SetState(TResponse::UNKNOWN);
        Result.ClearLeaderNodeId();
        Result.ClearLeaderGeneration();
        Result.SetMessage(
            "Leader connection was lost; tablet state is unknown");
        MaybeReply(ctx);
    }

    void HandleWhiteboardResponse(
        const TEvWhiteboard::TEvTabletStateResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        if (WhiteboardDone) {
            return;
        }
        WhiteboardDone = true;

        if (!ev->Get()->Record.GetPacked5().empty()) {
            Result.SetLocalStateMessage(
                "Unexpected packed whiteboard response");
            MaybeReply(ctx);
            return;
        }

        const TWhiteboardInfo* found = nullptr;
        for (const auto& info: ev->Get()->Record.GetTabletStateInfo()) {
            if (info.GetTabletId() != Result.GetTabletId() ||
                info.GetFollowerId() != 0)
            {
                continue;
            }
            if (found) {
                Result.SetLocalStateMessage(
                    "Conflicting local whiteboard records");
                MaybeReply(ctx);
                return;
            }
            found = &info;
        }

        if (found) {
            Result.SetLocalState(GetLocalState(*found));
            Result.SetLocalGeneration(found->GetGeneration());
            Result.SetLocalStateMessage(
                "Last local whiteboard observation; not permission to boot");
        } else {
            Result.SetLocalState(TResponse::LOCAL_NOT_OBSERVED);
            Result.SetLocalStateMessage(
                "No local primary tablet record; this does not prove the "
                "tablet is stopped or absent on other nodes");
        }
        MaybeReply(ctx);
    }

    void HandleBootInfosResponse(
        const TEvHiveProxy::TEvGetTabletBootInfosResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        if (BackupDone) {
            return;
        }
        BackupDone = true;
        const auto& msg = *ev->Get();

        if (HasError(msg.GetError())) {
            if (msg.GetStatus() == E_PRECONDITION_FAILED) {
                Result.SetBootInfoState(TResponse::BOOT_INFO_NOT_CONFIGURED);
                Result.SetBootInfoMessage(
                    "Local tablet boot info backup is not configured");
            } else {
                Result.SetBootInfoMessage(FormatError(msg.GetError()));
            }
        } else if (msg.TabletBootInfos.empty()) {
            Result.SetBootInfoState(TResponse::BOOT_INFO_MISSING);
            Result.SetBootInfoMessage("No entry in the loaded local backup");
        } else if (
            msg.TabletBootInfos.size() == 1 &&
            msg.TabletBootInfos.front().StorageInfoProto.GetTabletID() ==
                Result.GetTabletId())
        {
            Result.SetBootInfoState(TResponse::BOOT_INFO_AVAILABLE);
            Result.SetSuggestedGeneration(
                msg.TabletBootInfos.front().SuggestedGeneration);
            Result.SetBootInfoMessage(
                "Entry exists in the loaded local backup");
        } else {
            Result.SetBootInfoMessage("Unexpected local backup response");
        }
        MaybeReply(ctx);
    }

    void HandleUndelivered(
        const TEvents::TEvUndelivered::TPtr& ev,
        const TActorContext& ctx)
    {
        switch (ev->Get()->SourceType) {
            case TEvWhiteboard::TEvTabletStateRequest::EventType:
                if (!WhiteboardDone) {
                    WhiteboardDone = true;
                    Result.SetLocalStateMessage(
                        "Local whiteboard is unavailable");
                }
                break;
            case TEvHiveProxy::TEvGetTabletBootInfosRequest::EventType:
                if (!BackupDone) {
                    BackupDone = true;
                    Result.SetBootInfoMessage(
                        "Local backup service is unavailable");
                }
                break;
        }
        MaybeReply(ctx);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletPipe::TEvClientConnected, HandleClientConnected);
            HFunc(TEvTabletPipe::TEvClientDestroyed, HandleClientDestroyed);
            HFunc(
                TEvWhiteboard::TEvTabletStateResponse,
                HandleWhiteboardResponse);
            HFunc(
                TEvHiveProxy::TEvGetTabletBootInfosResponse,
                HandleBootInfosResponse);
            HFunc(TEvents::TEvUndelivered, HandleUndelivered);
            CFunc(TEvents::TEvWakeup::EventType, ReplyAndDie);
            CFunc(TEvents::TEvPoisonPill::EventType, ReplyAndDie);
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

TResultOrError<IActorPtr> TServiceActor::CreateGetTabletStateActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    NPrivateProto::TGetTabletStateRequest request;
    if (!google::protobuf::util::JsonStringToMessage(input, &request).ok()) {
        return MakeError(E_ARGUMENT, "Failed to parse GetTabletState input");
    }
    if (!request.GetTabletId()) {
        return MakeError(E_ARGUMENT, "TabletId must not be zero");
    }
    if (request.GetTimeoutMs() > MaxTimeoutMs) {
        return MakeError(E_ARGUMENT, "TimeoutMs must not exceed 30000");
    }

    return {std::make_unique<TGetTabletStateActor>(
        std::move(requestInfo),
        request)};
}

}   // namespace NCloud::NBlockStore::NStorage
