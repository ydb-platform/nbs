#include "public.h"

#include "disk_registry_proxy_private.h"

#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <ydb/core/base/tablet_pipe.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/mon.h>

#include <util/generic/deque.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDiskRegistryProxyActor final
    : public NActors::TActorBootstrapped<TDiskRegistryProxyActor>
{
private:
    using TActiveRequests = THashMap<ui64, NActors::IEventHandlePtr>;

private:
    const TStorageConfigPtr StorageConfig;
    const TDiskRegistryProxyConfigPtr Config;

    ui64 DiskRegistryTabletId = 0;
    NActors::TActorId TabletClientId;

    ui64 RequestId = 0;

    TActiveRequests ActiveRequests;

    TDeque<NActors::TActorId> Subscribers;

    TRequestInfoPtr ReassignRequestInfo;

public:
    TDiskRegistryProxyActor(
        TStorageConfigPtr config,
        TDiskRegistryProxyConfigPtr diskRegistryProxyConfig);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void CancelActiveRequests(const NActors::TActorContext& ctx);

    bool ReplyWithError(
        const NActors::TActorContext& ctx,
        TAutoPtr<NActors::IEventHandle>& ev);
    template <typename TMethod>
    void ReplyWithErrorImpl(
        const NActors::TActorContext& ctx,
        TAutoPtr<NActors::IEventHandle>& ev);

    void CreateClient(const NActors::TActorContext& ctx);

    void StartWork(ui64 tabletId, const NActors::TActorContext& ctx);

    void RegisterPages(const NActors::TActorContext& ctx);
    void RenderHtmlInfo(IOutputStream& out) const;

    void NotifySubscribersConnectionLost(const NActors::TActorContext& ctx);
    void NotifySubscribersConnectionEstablished(
        const NActors::TActorContext& ctx);

    void LookupTablet(const NActors::TActorContext& ctx);
    void CreateTablet(
        const NActors::TActorContext& ctx,
        TDiskRegistryChannelKinds kinds);

private:
    STFUNC(StateLookup);
    STFUNC(StateError);
    STFUNC(StateWork);

    void HandleConnected(
        NKikimr::TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDisconnect(
        NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRequest(
        const NActors::TActorContext& ctx,
        TAutoPtr<NActors::IEventHandle>& ev);

    void HandleResponse(
        const NActors::TActorContext& ctx,
        TAutoPtr<NActors::IEventHandle>& ev);

    void HandleLookupTabletResponse(
        NCloud::NStorage::TEvHiveProxy::TEvLookupTabletResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleHttpInfo(
        const NActors::NMon::TEvHttpInfo::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleCreateResult(
        const TEvDiskRegistryProxyPrivate::TEvDiskRegistryCreateResult::TPtr&
            ev,
        const NActors::TActorContext& ctx);

    void HandleLookupTablet(
        const TEvDiskRegistryProxyPrivate::TEvLookupTabletRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleCreateTablet(
        const TEvDiskRegistryProxyPrivate::TEvCreateTabletRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);
    bool LogLateMessage(STFUNC_SIG);

    BLOCKSTORE_DISK_REGISTRY_PROXY_REQUESTS(
        BLOCKSTORE_IMPLEMENT_REQUEST,
        TEvDiskRegistryProxy)
};

}   // namespace NCloud::NBlockStore::NStorage
