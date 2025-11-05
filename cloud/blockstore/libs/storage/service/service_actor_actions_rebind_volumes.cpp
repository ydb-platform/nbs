#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

#include <util/string/printf.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRebindVolumesActor final
    : public TActorBootstrapped<TRebindVolumesActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const NPrivateProto::TRebindVolumesRequest RebindRequest;
    const TVector<TString> DiskIds;
    ui32 Responses = 0;

    NProto::TError Error;

public:
    TRebindVolumesActor(
        TRequestInfoPtr requestInfo,
        NPrivateProto::TRebindVolumesRequest rebindRequest,
        TVector<TString> diskIds);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleChangeVolumeBindingResponse(
        const TEvService::TEvChangeVolumeBindingResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TRebindVolumesActor::TRebindVolumesActor(
        TRequestInfoPtr requestInfo,
        NPrivateProto::TRebindVolumesRequest rebindRequest,
        TVector<TString> diskIds)
    : RequestInfo(std::move(requestInfo))
    , RebindRequest(std::move(rebindRequest))
    , DiskIds(std::move(diskIds))
{}

void TRebindVolumesActor::Bootstrap(const TActorContext& ctx)
{
    const auto binding =
        static_cast<NProto::EVolumeBinding>(RebindRequest.GetBinding());

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "Rebinding %u disks, binding: %u",
        DiskIds.size(),
        binding);

    Become(&TThis::StateWork);

    for (const auto& diskId: DiskIds) {
        using TRequest = TEvService::TEvChangeVolumeBindingRequest;

        auto op = TRequest::EChangeBindingOp::ACQUIRE_FROM_HIVE;
        if (binding == NProto::BINDING_REMOTE) {
            op = TRequest::EChangeBindingOp::RELEASE_TO_HIVE;
        }

        auto request = std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
            diskId,
            op,
            NProto::SOURCE_MANUAL
        );

        NCloud::Send(ctx, MakeStorageServiceId(), std::move(request));
    }
}

void TRebindVolumesActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(Error);
    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TRebindVolumesResponse(),
        response->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_rebindvolumes",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TRebindVolumesActor::HandleChangeVolumeBindingResponse(
    const TEvService::TEvChangeVolumeBindingResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        // RebindVolumes request should always succeed, but we should collect
        // error messages for debugging purposes.

        const auto formattedError = FormatError(msg->GetError());

        LOG_WARN(ctx, TBlockStoreComponents::SERVICE,
            "Failed to rebind disk: %s",
            formattedError.Quote().c_str());

        TStringBuilder message;
        message << Error.GetMessage();
        if (message) {
            message << "; ";
        }

        message << formattedError;

        Error.SetMessage(std::move(message));
    }

    if (++Responses == DiskIds.size()) {
        LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
            "Rebound %u disks", DiskIds.size());

        ReplyAndDie(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TRebindVolumesActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvService::TEvChangeVolumeBindingResponse,
            HandleChangeVolumeBindingResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateRebindVolumesActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    TVector<TString> localDiskIds;
    for (const auto& x: State.GetVolumes()) {
        if (x.second->IsLocallyMounted()) {
            localDiskIds.push_back(x.first);
        }
    }

    NPrivateProto::TRebindVolumesRequest rebindRequest;
    if (!google::protobuf::util::JsonStringToMessage(input, &rebindRequest).ok()) {
        return MakeError(E_ARGUMENT, "Failed to parse input");
    }

    if (rebindRequest.GetBinding() > TVolumeInfo::MAX_VOLUME_BINDING) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Bad binding: " << rebindRequest.GetBinding());
    }

    const auto binding =
        static_cast<NProto::EVolumeBinding>(rebindRequest.GetBinding());

    if (localDiskIds.empty()) {
        return MakeError(S_ALREADY, "No disks - nothing to do");
    }

    if (binding == NProto::BINDING_NOT_SET) {
        return MakeError(E_ARGUMENT, "Binding is not set");
    }

    State.SetIsManuallyPreemptedVolumesTrackingDisabled(
        binding == NProto::BINDING_REMOTE);

    return {std::make_unique<TRebindVolumesActor>(
        std::move(requestInfo),
        std::move(rebindRequest),
        std::move(localDiskIds))};
}

}   // namespace NCloud::NBlockStore::NStorage
