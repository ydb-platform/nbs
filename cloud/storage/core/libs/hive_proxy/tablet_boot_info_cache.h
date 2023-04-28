#pragma once

#include "public.h"

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/hive_proxy/hive_proxy_events_private.h>
#include <cloud/storage/core/libs/hive_proxy/protos/tablet_boot_info_cache.pb.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>
#include <cloud/storage/core/libs/kikimr/public.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>

#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/file.h>
#include <util/system/file_lock.h>

#include <memory>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TTabletBootInfo
{
    TTabletBootInfo() = default;

    TTabletBootInfo(
            NKikimr::TTabletStorageInfoPtr storageInfo,
            ui64 suggestedGeneration)
        : StorageInfo(std::move(storageInfo))
        , SuggestedGeneration(suggestedGeneration)
    {}

    NKikimr::TTabletStorageInfoPtr StorageInfo;
    ui64 SuggestedGeneration = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TTabletBootInfoCache final
    : public NActors::TActorBootstrapped<TTabletBootInfoCache>
{
private:
    struct TRequestInfo
    {
        NActors::TActorId Sender;
        ui64 Cookie = 0;

        TRequestInfo(NActors::TActorId sender, ui64 cookie)
            : Sender(sender)
            , Cookie(cookie)
        {}
    };

private:
    int LogComponent;
    const TFsPath CacheFilePath;
    IFileIOServicePtr FileIOService;
    const bool SyncEnabled = false;

    NHiveProxy::NProto::TTabletBootInfoCache Cache;

    const TFsPath TmpCacheFilePath;
    std::unique_ptr<TFileHandle> TmpCacheFileHandle;
    std::unique_ptr<TFileLock> TmpCacheFileLock;
    TString TmpCacheFileBuffer;

    bool SyncInProgress = false;
    TVector<TRequestInfo> SyncRequests;

public:
    // Never reads from cache file when sync is enabled, just overwrites its
    // content.
    TTabletBootInfoCache(
        int logComponent,
        TString cacheFilePath,
        IFileIOServicePtr fileIO,
        bool syncEnabled);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void ScheduleSync(const NActors::TActorContext& ctx);
    void Sync(const NActors::TActorContext& ctx);
    void SyncCompleted(
        const NActors::TActorContext& ctx,
        NProto::TError error);

    void HandleCompleted(
        const NActors::TEvents::TEvCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadTabletBootInfoCache(
        const TEvHiveProxyPrivate::TEvReadTabletBootInfoCacheRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateTabletBootInfoCache(
        const TEvHiveProxyPrivate::TEvUpdateTabletBootInfoCacheRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleSyncTabletBootInfoCache(
        const TEvHiveProxy::TEvSyncTabletBootInfoCacheRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NStorage
