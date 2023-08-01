#include "tablet_boot_info_cache.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/kikimr/components.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/system/file.h>
#include <util/system/file_lock.h>

namespace NCloud::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration SyncInterval = TDuration::Seconds(10);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TTabletBootInfoCache::TTabletBootInfoCache(
        int logComponent,
        TString cacheFilePath,
        bool syncEnabled)
    : LogComponent(logComponent)
    , CacheFilePath(std::move(cacheFilePath))
    , SyncEnabled(syncEnabled)
    , TmpCacheFilePath(CacheFilePath.GetPath() + ".tmp")
{
    ActivityType = TStorageActivities::HIVE_PROXY;
}

void TTabletBootInfoCache::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    if (SyncEnabled) {
        ScheduleSync(ctx);
    } else {
        // Read only mode.
        try {
            MergeFromTextFormat(CacheFilePath, Cache);
        } catch (...) {
            LOG_WARN_S(ctx, LogComponent,
                "TabletBootInfoCache: can't load from cache file: "
                << CurrentExceptionMessage());
        }
    }

    LOG_INFO_S(ctx, LogComponent,
        "TabletBootInfoCache: started with SyncEnabled=" << SyncEnabled);
}

void TTabletBootInfoCache::ScheduleSync(const TActorContext& ctx)
{
    ctx.Schedule(SyncInterval, new TEvents::TEvWakeup());
}

NProto::TError TTabletBootInfoCache::Sync(const TActorContext& ctx)
{
    NProto::TError error;

    try {
        TFileLock lock(TmpCacheFilePath);

        if (lock.TryAcquire()) {
            TFileOutput output(TmpCacheFilePath);
            SerializeToTextFormat(Cache, output);
            TmpCacheFilePath.RenameTo(CacheFilePath);
        } else {
            auto message = TStringBuilder()
                << "failed to acquire lock on file: " << TmpCacheFilePath;
            error = MakeError(E_IO, std::move(message));
        }
    } catch (...) {
        error = MakeError(E_FAIL, CurrentExceptionMessage());
    }

    if (SUCCEEDED(error.GetCode())) {
        LOG_DEBUG_S(ctx, LogComponent, "TabletBootInfoCache: sync completed");
    } else {
        ReportTabletBootInfoCacheSyncFailure();

        LOG_ERROR_S(ctx, LogComponent,
            "TabletBootInfoCache: sync failed: "
            << error);

        try {
            TmpCacheFilePath.DeleteIfExists();
        } catch (...) {
            LOG_WARN_S(ctx, LogComponent,
                "TabletBootInfoCache: failed to delete temporary file: "
                << CurrentExceptionMessage());
        }
    }

    return error;
}

////////////////////////////////////////////////////////////////////////////////

void TTabletBootInfoCache::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Sync(ctx);
    ScheduleSync(ctx);
}

void TTabletBootInfoCache::HandleReadTabletBootInfoCache(
    const TEvHiveProxyPrivate::TEvReadTabletBootInfoCacheRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvHiveProxyPrivate::TEvReadTabletBootInfoCacheResponse;

    auto* msg = ev->Get();

    std::unique_ptr<TResponse> response;

    auto it = Cache.GetData().find(msg->TabletId);
    if (it == Cache.GetData().end()) {
        LOG_DEBUG_S(ctx, LogComponent,
            "TabletBootInfoCache: no data for tablet " << msg->TabletId);
        response = std::make_unique<TResponse>(MakeError(E_NOT_FOUND));
    } else {
        LOG_DEBUG_S(ctx, LogComponent,
            "TabletBootInfoCache: read data for tablet " << msg->TabletId);

        auto storageInfo = it->second.GetStorageInfo();
        auto suggestedGeneration = it->second.GetSuggestedGeneration();
        response = std::make_unique<TResponse>(
            NKikimr::TabletStorageInfoFromProto(std::move(storageInfo)),
            suggestedGeneration);
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TTabletBootInfoCache::HandleUpdateTabletBootInfoCache(
    const TEvHiveProxyPrivate::TEvUpdateTabletBootInfoCacheRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    NHiveProxy::NProto::TTabletBootInfo info;
    NKikimr::TabletStorageInfoToProto(
        *msg->StorageInfo, info.MutableStorageInfo());
    info.SetSuggestedGeneration(msg->SuggestedGeneration);

    auto& data = *Cache.MutableData();
    data[msg->StorageInfo->TabletID] = info;

    LOG_DEBUG_S(ctx, LogComponent,
        "TabletBootInfoCache: updated data for tablet "
            << msg->StorageInfo->TabletID);
}

void TTabletBootInfoCache::HandleSyncTabletBootInfoCache(
    const TEvHiveProxy::TEvSyncTabletBootInfoCacheRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvHiveProxy::TEvSyncTabletBootInfoCacheResponse;

    NProto::TError error;
    if (SyncEnabled) {
        error = Sync(ctx);
    } else {
        error = MakeError(E_PRECONDITION_FAILED, "sync is disabled");
    }

    auto response = std::make_unique<TResponse>(std::move(error));
    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TTabletBootInfoCache::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvHiveProxyPrivate::TEvReadTabletBootInfoCacheRequest, HandleReadTabletBootInfoCache);
        HFunc(TEvHiveProxyPrivate::TEvUpdateTabletBootInfoCacheRequest, HandleUpdateTabletBootInfoCache);
        HFunc(TEvHiveProxy::TEvSyncTabletBootInfoCacheRequest, HandleSyncTabletBootInfoCache);
        default:
            HandleUnexpectedEvent(ev, LogComponent);
            break;
    }
}

}   // namespace NCloud::NStorage
