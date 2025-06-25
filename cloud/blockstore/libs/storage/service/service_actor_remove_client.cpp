#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRemoveClientActor final: public TActorBootstrapped<TRemoveClientActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString ClientId;
    const TString DiskId;

public:
    TRemoveClientActor(
            TRequestInfoPtr requestInfo,
            TString clientId,
            TString diskId)
        : RequestInfo(std::move(requestInfo))
        , ClientId(std::move(clientId))
        , DiskId(std::move(diskId))
    {}

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleRemoveClientResponse(
        const TEvVolume::TEvRemoveClientResponse::TPtr& ev,
        const TActorContext& ctx);
};

void TRemoveClientActor::Bootstrap(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvRemoveClientRequest>();
    request->Record.SetDiskId(DiskId);
    request->Record.MutableHeaders()->SetClientId(ClientId);

    ctx.Send(MakeVolumeProxyServiceId(), std::move(request));
    Become(&TThis::StateWork);
}

void TRemoveClientActor::HandleRemoveClientResponse(
    const TEvVolume::TEvRemoveClientResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Error while removing client %s from disk %s : %s",
            ClientId.Quote().c_str(),
            DiskId.Quote().c_str(),
            FormatError(msg->GetError()).c_str());
    }

    auto response = std::make_unique<TEvService::TEvRemoveVolumeClientResponse>();
    *response->Record.MutableError() = msg->GetError();

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

STFUNC(TRemoveClientActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvRemoveClientResponse, HandleRemoveClientResponse);

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

void TServiceActor::HandleRemoveVolumeClient(
    const TEvService::TEvRemoveVolumeClientRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto& request = ev->Get()->Record;
    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "RemoveClient diskID: %s, clientId: %s",
        request.GetDiskId().Quote().c_str(),
        request.GetHeaders().GetClientId().Quote().c_str());

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext);

    NCloud::Register<TRemoveClientActor>(
        ctx,
        std::move(requestInfo),
        std::move(*request.MutableHeaders()->MutableClientId()),
        std::move(*request.MutableDiskId()));
}

}   // namespace NCloud::NBlockStore::NStorage
