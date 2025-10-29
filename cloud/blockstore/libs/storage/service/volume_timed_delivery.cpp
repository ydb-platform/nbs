#include "service_actor.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/mount_token.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/service/service_events_private.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
class TVolumeProxyTimedDeliveryActor final
    : public TActorBootstrapped<TVolumeProxyTimedDeliveryActor<TMethod>>
{
    using TThis = TVolumeProxyTimedDeliveryActor<TMethod>;
private:
    std::unique_ptr<typename TMethod::TRequest> Request;
    const TRequestInfoPtr RequestInfo;
    const TDuration Timeout;
    const TDuration BackoffIncrement;

    TActorId VolumeProxy;

    NProto::TError LastError;
    TDuration CurrentBackoff;

public:
    TVolumeProxyTimedDeliveryActor(
            std::unique_ptr<typename TMethod::TRequest> request,
            TRequestInfoPtr requestInfo,
            TDuration timeout,
            TDuration backoffIncrement,
            TActorId volumeProxy)
        : Request(std::move(request))
        , RequestInfo(std::move(requestInfo))
        , Timeout(timeout)
        , BackoffIncrement(backoffIncrement)
        , VolumeProxy(volumeProxy)
    {}

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRequest(const TActorContext& ctx);

    void HandleResponse(
        const typename TMethod::TResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void ReplyErrorAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TVolumeProxyTimedDeliveryActor<TMethod>::Bootstrap(const TActorContext& ctx)
{
    TThis::Become(&TThis::StateWork);
    SendRequest(ctx);
    ctx.Schedule(Timeout, new TEvents::TEvWakeup(false));
}

template <typename TMethod>
void TVolumeProxyTimedDeliveryActor<TMethod>::SendRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<typename TMethod::TRequest>();
    request->Record = Request->Record;

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "Sending %s %s to volume %s",
        TMethod::Name,
        Request->Record.GetHeaders().GetClientId().Quote().c_str(),
        Request->Record.GetDiskId().Quote().c_str());

    ctx.Send(
        VolumeProxy,
        request.release(),
        0,
        RequestInfo->Cookie);
}

template <typename TMethod>
void TVolumeProxyTimedDeliveryActor<TMethod>::HandleResponse(
    const typename TMethod::TResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (msg->Record.GetError().GetCode() == E_REJECTED) {
        LOG_WARN(ctx, TBlockStoreComponents::SERVICE,
            "%s %s request sent to volume %s is rejected",
            TMethod::Name,
            Request->Record.GetHeaders().GetClientId().Quote().c_str(),
            Request->Record.GetDiskId().Quote().c_str());

        LastError = msg->Record.GetError();
        CurrentBackoff += BackoffIncrement;
        ctx.Schedule(CurrentBackoff, new TEvents::TEvWakeup(true));
        return;
    }
    ctx.Send(ev->Forward(RequestInfo->Sender));
    TThis::Die(ctx);
}

template <typename TMethod>
void TVolumeProxyTimedDeliveryActor<TMethod>::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (msg->Tag) {
        LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
            "Resend %s %s request to volume %s. Last error %s",
            TMethod::Name,
            Request->Record.GetHeaders().GetClientId().Quote().c_str(),
            Request->Record.GetDiskId().Quote().c_str(),
            LastError.GetMessage().Quote().c_str());

        SendRequest(ctx);
        return;
    }

    LOG_WARN(ctx, TBlockStoreComponents::SERVICE,
        "%s %s request sent to volume %s timed out. Last error %s",
        TMethod::Name,
        Request->Record.GetHeaders().GetClientId().Quote().c_str(),
        Request->Record.GetDiskId().Quote().c_str(),
        LastError.GetMessage().Quote().c_str());

    ReplyErrorAndDie(
        ctx,
        MakeError(
            E_REJECTED,
            TStringBuilder() <<
                "Failed to deliver " <<
                TMethod::Name <<
                " request to volume"));
}

template <typename TMethod>
void TVolumeProxyTimedDeliveryActor<TMethod>::ReplyErrorAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<typename TMethod::TResponse>(error);

    NCloud::Send(ctx, RequestInfo->Sender, std::move(response));
    TThis::Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
STFUNC(TVolumeProxyTimedDeliveryActor<TMethod>::StateWork)
{
    switch (ev->GetTypeRewrite()) {

        HFunc(TMethod::TResponse, HandleResponse);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

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

IActorPtr CreateAddClientActor(
    std::unique_ptr<TEvVolume::TEvAddClientRequest> request,
    TRequestInfoPtr requestInfo,
    TDuration timeout,
    TDuration backoffTimeoutIncrement,
    TActorId volumeProxy)
{
    using TActorType = TVolumeProxyTimedDeliveryActor<TEvVolume::TAddClientMethod>;
    return std::make_unique<TActorType>(
        std::move(request),
        std::move(requestInfo),
        timeout,
        backoffTimeoutIncrement,
        volumeProxy);
}

IActorPtr CreateWaitReadyActor(
    std::unique_ptr<TEvVolume::TEvWaitReadyRequest> request,
    TRequestInfoPtr requestInfo,
    TDuration timeout,
    TDuration backoffTimeoutIncrement,
    TActorId volumeProxy)
{
    using TActorType = TVolumeProxyTimedDeliveryActor<TEvVolume::TWaitReadyMethod>;
    return std::make_unique<TActorType>(
        std::move(request),
        std::move(requestInfo),
        timeout,
        backoffTimeoutIncrement,
        volumeProxy);
}


}   // namespace NCloud::NBlockStore::NStorage
