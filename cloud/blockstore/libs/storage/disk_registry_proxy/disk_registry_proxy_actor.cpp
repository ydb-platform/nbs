#include "disk_registry_proxy_actor.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/core/tenant.h>
#include <cloud/blockstore/libs/storage/disk_registry_proxy/model/config.h>
#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

////////////////////////////////////////////////////////////////////////////////

TDiskRegistryProxyActor::TDiskRegistryProxyActor(
        TStorageConfigPtr config,
        TDiskRegistryProxyConfigPtr diskRegistryProxyConfig)
    : StorageConfig(std::move(config))
    , Config(std::move(diskRegistryProxyConfig))
{}

void TDiskRegistryProxyActor::Bootstrap(const TActorContext& ctx)
{
    if (!Config->GetOwner()) {
        LOG_WARN(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
            "Disk Registry Proxy not configured");

        TThis::Become(&TThis::StateError);
        return;
    }

    TThis::Become(&TThis::StateLookup);

    RegisterPages(ctx);

    LookupTablet(ctx);
}

void TDiskRegistryProxyActor::LookupTablet(const TActorContext& ctx)
{
    if (ui64 diskRegistryTabletId = Config->GetDiskRegistryTabletId()) {
        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_PROXY,
            "Got Disk Registry tablet id from config: "
                << diskRegistryTabletId);
        StartWork(diskRegistryTabletId, ctx);
        return;
    }

    const auto hiveTabletId = GetHiveTabletId(StorageConfig, ctx);

    LOG_INFO_S(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
        "Lookup Disk Registry tablet. Hive: " << hiveTabletId);

    const ui64 cookie = ++RequestId;
    NCloud::Send<TEvHiveProxy::TEvLookupTabletRequest>(
        ctx,
        MakeHiveProxyServiceId(),
        cookie,
        hiveTabletId,
        Config->GetOwner(),
        Config->GetOwnerIdx());

    ctx.Schedule(
        Config->GetLookupTimeout(),
        std::make_unique<IEventHandle>(
            ctx.SelfID,
            ctx.SelfID,
            new TEvents::TEvWakeup(),
            0,  // flags
            cookie));
}

void TDiskRegistryProxyActor::StartWork(
    ui64 tabletId,
    const TActorContext& ctx)
{
    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
        "Ready to work. Tablet ID: %lu", tabletId);

    TThis::Become(&TThis::StateWork);

    DiskRegistryTabletId = tabletId;
    NotifySubscribersConnectionEstablished(ctx);
}

void TDiskRegistryProxyActor::CreateClient(const TActorContext& ctx)
{
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = StorageConfig->GetPipeClientRetryCount(),
        .MinRetryTime = StorageConfig->GetPipeClientMinRetryTime(),
        .MaxRetryTime = StorageConfig->GetPipeClientMaxRetryTime()
    };

    TabletClientId = ctx.Register(NTabletPipe::CreateClient(
        ctx.SelfID,
        DiskRegistryTabletId,
        clientConfig));

    LOG_INFO(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
        "Tablet client: %lu (remote: %s)",
        DiskRegistryTabletId,
        ToString(TabletClientId).data());
}

bool TDiskRegistryProxyActor::ReplyWithError(
    const TActorContext& ctx,
    TAutoPtr<IEventHandle>& ev)
{
#define BLOCKSTORE_HANDLE_METHOD(name, ns)                                     \
    case ns::TEv##name##Request::EventType: {                                  \
        ReplyWithErrorImpl<ns::T##name##Method>(ctx, ev);                      \
        return true;                                                           \
    }                                                                          \
// BLOCKSTORE_HANDLE_METHOD

    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_DISK_REGISTRY_REQUESTS_PROTO(BLOCKSTORE_HANDLE_METHOD,
                TEvDiskRegistry)
        BLOCKSTORE_DISK_REGISTRY_REQUESTS_FWD_SERVICE(BLOCKSTORE_HANDLE_METHOD,
                TEvService)
        BLOCKSTORE_DISK_REGISTRY_PROXY_REQUESTS(BLOCKSTORE_HANDLE_METHOD,
                TEvDiskRegistryProxy)
        default:
            return false;
    }
#undef BLOCKSTORE_HANDLE_METHOD
}

template <typename TMethod>
void TDiskRegistryProxyActor::ReplyWithErrorImpl(
    const TActorContext& ctx,
    TAutoPtr<IEventHandle>& ev)
{
    auto response = std::make_unique<typename TMethod::TResponse>(
        MakeError(E_REJECTED, "DiskRegistry tablet not available"));

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TDiskRegistryProxyActor::CancelActiveRequests(const TActorContext& ctx)
{
    TActiveRequests activeRequests = std::move(ActiveRequests);

    for (auto& kv : activeRequests) {
        TAutoPtr<IEventHandle> ev(kv.second.release());

        if (!ReplyWithError(ctx, ev)) {
            Y_ABORT("Unexpected event: (0x%08X)", ev->GetTypeRewrite());
        }
    }
}

void TDiskRegistryProxyActor::RegisterPages(const TActorContext& ctx)
{
    auto mon = AppData(ctx)->Mon;
    if (mon) {
        auto* rootPage = mon->RegisterIndexPage("blockstore", "BlockStore");

        mon->RegisterActorPage(rootPage, "disk_registry_proxy", "DiskRegistryProxy",
            false, ctx.ActorSystem(), SelfId());
    }
}

void TDiskRegistryProxyActor::NotifySubscribersConnectionLost(
    const TActorContext& ctx)
{
    for (const auto& subscriber: Subscribers) {
        auto request =
            std::make_unique<TEvDiskRegistryProxy::TEvConnectionLost>();

        NCloud::Send(ctx, subscriber, std::move(request));
    }
}

void TDiskRegistryProxyActor::NotifySubscribersConnectionEstablished(
    const TActorContext& ctx)
{
    for (const auto& subscriber: Subscribers) {
        auto request =
            std::make_unique<TEvDiskRegistryProxy::TEvConnectionEstablished>();

        NCloud::Send(ctx, subscriber, std::move(request));
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryProxyActor::HandleConnected(
    TEvTabletPipe::TEvClientConnected::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_INFO_S(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
        "Connection to Disk Registry ready. TabletId: " << msg->TabletId
            << " Status: " << NKikimrProto::EReplyStatus_Name(msg->Status)
            << " ClientId: " << msg->ClientId
            << " ServerId: " << msg->ServerId
            << " Master: " << msg->Leader
            << " Dead: " << msg->Dead);

    if (msg->Status != NKikimrProto::OK) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
            "Cannot connect to Disk Registry tablet " << msg->TabletId << ": "
                << NKikimrProto::EReplyStatus_Name(msg->Status));

        TabletClientId = {};
        CancelActiveRequests(ctx);
    }
}

void TDiskRegistryProxyActor::HandleDisconnect(
    TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Y_ABORT_UNLESS(DiskRegistryTabletId);

    TabletClientId = {};

    const auto error = MakeError(E_REJECTED, "Connection broken");

    LOG_WARN(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
        "Connection to Disk Registry failed: %s",
        FormatError(error).data());

    NotifySubscribersConnectionLost(ctx);

    CancelActiveRequests(ctx);
}

void TDiskRegistryProxyActor::HandleRequest(
    const TActorContext& ctx,
    TAutoPtr<IEventHandle>& ev)
{
    Y_ABORT_UNLESS(DiskRegistryTabletId);

    if (!TabletClientId) {
        CreateClient(ctx);
    }

    const ui64 requestId = ++RequestId;

    auto event = std::make_unique<IEventHandle>(
        ev->Recipient,
        SelfId(),
        ev->ReleaseBase().Release(),
        0,          // flags
        requestId); // cookie
    NCloud::PipeSend(ctx, TabletClientId, std::move(event));

    ActiveRequests.emplace(requestId, IEventHandlePtr(ev.Release()));
}

void TDiskRegistryProxyActor::HandleResponse(
    const TActorContext& ctx,
    TAutoPtr<IEventHandle>& ev)
{
    auto it = ActiveRequests.find(ev->Cookie);
    if (it == ActiveRequests.end()) {
        // ActiveRequests are cleared upon connection reset
        if (!LogLateMessage(ev)) {
            LogUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_PROXY,
                __PRETTY_FUNCTION__);
        }
        return;
    }

    IEventHandle& request = *it->second;

    // forward response to the caller
    TAutoPtr<IEventHandle> event;
    if (ev->HasEvent()) {
        event = new IEventHandle(
            request.Sender,
            ev->Sender,
            ev->ReleaseBase().Release(),
            ev->Flags,
            request.Cookie);
    } else {
        event = new IEventHandle(
            ev->Type,
            ev->Flags,
            request.Sender,
            ev->Sender,
            ev->ReleaseChainBuffer(),
            request.Cookie);
    }

    ctx.Send(event);
    ActiveRequests.erase(it);
}

bool TDiskRegistryProxyActor::HandleRequests(STFUNC_SIG)
{
    auto ctx(ActorContext());
#define BLOCKSTORE_HANDLE_METHOD(name, ns)                                     \
    case ns::TEv##name##Request::EventType: {                                  \
        HandleRequest(ctx, ev);                                                \
        break;                                                                 \
    }                                                                          \
    case ns::TEv##name##Response::EventType: {                                 \
        HandleResponse(ctx, ev);                                               \
        break;                                                                 \
    }                                                                          \
// BLOCKSTORE_HANDLE_METHOD

    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_DISK_REGISTRY_REQUESTS_PROTO(BLOCKSTORE_HANDLE_METHOD,
            TEvDiskRegistry)
        BLOCKSTORE_DISK_REGISTRY_REQUESTS_FWD_SERVICE(BLOCKSTORE_HANDLE_METHOD,
            TEvService)

        default:
            return false;
    }

    return true;

#undef BLOCKSTORE_HANDLE_METHOD
}

bool TDiskRegistryProxyActor::LogLateMessage(STFUNC_SIG)
{
    auto ctx(ActorContext());
#define BLOCKSTORE_LOG_MESSAGE(name, ns)                                       \
    case ns::TEv##name##Request::EventType: {                                  \
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,             \
            "Late request : (0x%08X) %s request",                              \
            ev->GetTypeRewrite(),                                              \
            #name);                                                            \
        break;                                                                 \
    }                                                                          \
    case ns::TEv##name##Response::EventType: {                                 \
        LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,             \
          "Late response : (0x%08X) %s response",                              \
          ev->GetTypeRewrite(),                                                \
          #name);                                                              \
        break;                                                                 \
    }                                                                          \
// BLOCKSTORE_LOG_MESSAGE

    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_DISK_REGISTRY_REQUESTS_PROTO(BLOCKSTORE_LOG_MESSAGE,
            TEvDiskRegistry)
        BLOCKSTORE_DISK_REGISTRY_REQUESTS_FWD_SERVICE(BLOCKSTORE_LOG_MESSAGE,
            TEvService)

        default:
            return false;
    }

    return true;

#undef BLOCKSTORE_LOG_MESSAGE
}

void TDiskRegistryProxyActor::HandleLookupTabletResponse(
    TEvHiveProxy::TEvLookupTabletResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (!HasError(msg->Error)) {
        StartWork(msg->TabletId, ctx);
        return;
    }

    if (GetErrorKind(msg->GetError()) == EErrorKind::ErrorRetriable) {
        LOG_WARN_S(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_PROXY,
            "LookupTablet failed with a retriable error: "
                << FormatError(msg->GetError()));

        ++RequestId;
        ctx.Schedule(
            Config->GetRetryLookupTimeout(),
            new TEvDiskRegistryProxyPrivate::TEvLookupTabletRequest());
        return;
    }

    if (ev->Cookie != RequestId) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_PROXY,
            "Received expired LookupTablet response");
        return;
    }

    LOG_WARN_S(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
        "Can't find Disk Registry tablet: " << FormatError(msg->Error)
            << ". Will try to create it");

    CreateTablet(ctx, {});
}

void TDiskRegistryProxyActor::RenderHtmlInfo(IOutputStream& out) const
{
    using namespace NMonitoringUtils;

    HTML(out) {
        out << "Disk Registry Tablet: ";
        if (DiskRegistryTabletId) {
            out << "<a href='../tablets?TabletID="
                << DiskRegistryTabletId << "'>"
                << DiskRegistryTabletId << "</a>";
        }

        TAG(TH3) { out << "Config"; }
        Config->DumpHtml(out);
    }
}

void TDiskRegistryProxyActor::HandleHttpInfo(
    const NMon::TEvHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, TBlockStoreComponents::DISK_REGISTRY_PROXY,
        "HTTP request: " << ev->Get()->Request.GetUri());

    TStringStream out;
    RenderHtmlInfo(out);

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<NMon::TEvHttpInfoRes>(out.Str()));
}

void TDiskRegistryProxyActor::HandleSubscribe(
    const TEvDiskRegistryProxy::TEvSubscribeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    NProto::TError error;

    auto* msg = ev->Get();
    auto it = Find(Subscribers, msg->Subscriber);
    if (it == Subscribers.end()) {
        Subscribers.push_back(msg->Subscriber);
    } else {
        error.SetCode(S_ALREADY);
    }

    const bool discovered = !!DiskRegistryTabletId;
    auto response =
        std::make_unique<TEvDiskRegistryProxy::TEvSubscribeResponse>(
            std::move(error),
            discovered);

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TDiskRegistryProxyActor::HandleUnsubscribe(
    const TEvDiskRegistryProxy::TEvUnsubscribeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    NProto::TError error;

    auto* msg = ev->Get();
    auto it = Find(Subscribers, msg->Subscriber);
    if (it != Subscribers.end()) {
        Subscribers.erase(it);
    } else {
        error.SetCode(S_FALSE);
    }

    auto response = std::make_unique<TEvDiskRegistryProxy::TEvUnsubscribeResponse>(
        std::move(error));

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TDiskRegistryProxyActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Cookie != RequestId) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_PROXY,
            "Received expired Lookup tablet timeout.");
        return;
    }

    LOG_ERROR(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY_PROXY,
        "Lookup tablet request timed out. Retry.");

    LookupTablet(ctx);
}

void TDiskRegistryProxyActor::HandleReassign(
    const TEvDiskRegistryProxy::TEvReassignRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (ReassignRequestInfo) {
        auto response = std::make_unique<TEvDiskRegistryProxy::TEvReassignResponse>(
            MakeError(E_REJECTED, "Disk Registry reassign is in progress"));

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto* msg = ev->Get();

    ReassignRequestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    CreateTablet(ctx, TDiskRegistryChannelKinds {
        std::move(msg->SysKind),
        std::move(msg->LogKind),
        std::move(msg->IndexKind)
    });
}

void TDiskRegistryProxyActor::HandleGetDrTabletInfo(
    const TEvDiskRegistryProxy::TEvGetDrTabletInfoRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvDiskRegistryProxy::TEvGetDrTabletInfoResponse>(
            DiskRegistryTabletId);

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TDiskRegistryProxyActor::HandleLookupTablet(
    const TEvDiskRegistryProxyPrivate::TEvLookupTabletRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    LookupTablet(ctx);
}

void TDiskRegistryProxyActor::HandleCreateTablet(
    const TEvDiskRegistryProxyPrivate::TEvCreateTabletRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    CreateTablet(ctx, std::move(msg->Kinds));
}

STFUNC(TDiskRegistryProxyActor::StateError)
{
    switch (ev->GetTypeRewrite()) {
        IgnoreFunc(TEvents::TEvWakeup);

        HFunc(NMon::TEvHttpInfo, HandleHttpInfo);

        HFunc(TEvDiskRegistryProxy::TEvGetDrTabletInfoRequest,
            RejectGetDrTabletInfo);

        IgnoreFunc(TEvHiveProxy::TEvLookupTabletResponse);
        IgnoreFunc(TEvHiveProxy::TEvCreateTabletResponse);
        IgnoreFunc(TEvDiskRegistryProxyPrivate::TEvLookupTabletRequest);
        IgnoreFunc(TEvDiskRegistryProxyPrivate::TEvCreateTabletRequest);

        default:
            if (!ReplyWithError(ActorContext(), ev)) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_REGISTRY_PROXY,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

STFUNC(TDiskRegistryProxyActor::StateLookup)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvHiveProxy::TEvLookupTabletResponse, HandleLookupTabletResponse);
        HFunc(
            TEvDiskRegistryProxyPrivate::TEvDiskRegistryCreateResult,
            HandleCreateResult);
        HFunc(
            TEvDiskRegistryProxyPrivate::TEvLookupTabletRequest,
            HandleLookupTablet);
        HFunc(
            TEvDiskRegistryProxyPrivate::TEvCreateTabletRequest,
            HandleCreateTablet);

        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(NMon::TEvHttpInfo, HandleHttpInfo);

        HFunc(TEvDiskRegistryProxy::TEvGetDrTabletInfoRequest,
            RejectGetDrTabletInfo);

        HFunc(TEvDiskRegistryProxy::TEvSubscribeRequest, HandleSubscribe);
        HFunc(TEvDiskRegistryProxy::TEvUnsubscribeRequest, HandleUnsubscribe);

        default:
            if (!ReplyWithError(ActorContext(), ev)) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_REGISTRY_PROXY,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

STFUNC(TDiskRegistryProxyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTabletPipe::TEvClientConnected, HandleConnected);
        HFunc(TEvTabletPipe::TEvClientDestroyed, HandleDisconnect);

        IgnoreFunc(TEvents::TEvWakeup);

        HFunc(NMon::TEvHttpInfo, HandleHttpInfo);

        HFunc(TEvDiskRegistryProxy::TEvGetDrTabletInfoRequest,
            HandleGetDrTabletInfo);

        IgnoreFunc(TEvHiveProxy::TEvLookupTabletResponse);
        HFunc(
            TEvDiskRegistryProxyPrivate::TEvDiskRegistryCreateResult,
            HandleCreateResult);
        IgnoreFunc(TEvDiskRegistryProxyPrivate::TEvLookupTabletRequest);
        IgnoreFunc(TEvDiskRegistryProxyPrivate::TEvCreateTabletRequest);

        HFunc(TEvDiskRegistryProxy::TEvSubscribeRequest, HandleSubscribe);
        HFunc(TEvDiskRegistryProxy::TEvUnsubscribeRequest, HandleUnsubscribe);

        HFunc(TEvDiskRegistryProxy::TEvReassignRequest, HandleReassign);

        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_REGISTRY_PROXY,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
