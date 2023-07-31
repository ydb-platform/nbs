#include "path_description_cache.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/system/file.h>
#include <util/system/file_lock.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration SyncInterval = TDuration::Seconds(10);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TPathDescriptionCache::TPathDescriptionCache(
        TString cacheFilePath,
        bool syncEnabled)
    : CacheFilePath(std::move(cacheFilePath))
    , SyncEnabled(syncEnabled)
    , TmpCacheFilePath(CacheFilePath.GetPath() + ".tmp")
{
    ActivityType = TBlockStoreActivities::SS_PROXY;
}

void TPathDescriptionCache::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    if (SyncEnabled) {
        ScheduleSync(ctx);
    } else {
        // Read only mode.
        try {
            MergeFromTextFormat(CacheFilePath, Cache);
        } catch (...) {
            LOG_WARN_S(ctx, TBlockStoreComponents::SS_PROXY,
                "PathDescriptionCache: can't load from cache file: "
                << CurrentExceptionMessage());
        }
    }

    LOG_INFO_S(ctx, TBlockStoreComponents::SS_PROXY,
        "PathDescriptionCache: started with SyncEnabled=" << SyncEnabled);
}

void TPathDescriptionCache::ScheduleSync(const TActorContext& ctx)
{
    ctx.Schedule(SyncInterval, new TEvents::TEvWakeup());
}

NProto::TError TPathDescriptionCache::Sync(const TActorContext& ctx)
{
    NProto::TError error;

    try {
        TFileLock lock(TmpCacheFilePath);

        if (lock.TryAcquire()) {
            TUnbufferedFileOutput output(TmpCacheFilePath);
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
        LOG_DEBUG_S(ctx, TBlockStoreComponents::SS_PROXY,
            "PathDescriptionCache: sync completed");
    } else {
        ReportPathDescriptionCacheSyncFailure();

        LOG_ERROR_S(ctx, TBlockStoreComponents::SS_PROXY,
            "PathDescriptionCache: sync failed: "
            << error);

        try {
            TmpCacheFilePath.DeleteIfExists();
        } catch (...) {
            LOG_WARN_S(ctx, TBlockStoreComponents::SS_PROXY,
                "PathDescriptionCache: failed to delete temporary file: "
                << CurrentExceptionMessage());
        }
    }

    return error;
}

////////////////////////////////////////////////////////////////////////////////

void TPathDescriptionCache::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Sync(ctx);
    ScheduleSync(ctx);
}

void TPathDescriptionCache::HandleReadPathDescriptionCache(
    const TEvSSProxyPrivate::TEvReadPathDescriptionCacheRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvSSProxyPrivate::TEvReadPathDescriptionCacheResponse;

    auto* msg = ev->Get();

    bool found = false;
    NKikimrSchemeOp::TPathDescription pathDescription;

    {
        auto it = Cache.GetData().find(msg->Path);
        if (it != Cache.GetData().end()) {
            found = true;
            pathDescription = it->second;
        }
    }

    std::unique_ptr<TResponse> response;

    if (found) {
        LOG_DEBUG_S(ctx, TBlockStoreComponents::SS_PROXY,
            "PathDescriptionCache: found data for path " << msg->Path);
        response = std::make_unique<TResponse>(
            std::move(msg->Path), std::move(pathDescription));
    } else {
        LOG_DEBUG_S(ctx, TBlockStoreComponents::SS_PROXY,
            "PathDescriptionCache: no data for path " << msg->Path);
        response = std::make_unique<TResponse>(MakeError(E_NOT_FOUND));
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TPathDescriptionCache::HandleUpdatePathDescriptionCache(
    const TEvSSProxyPrivate::TEvUpdatePathDescriptionCacheRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& data = *Cache.MutableData();
    data[msg->Path] = std::move(msg->PathDescription);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::SS_PROXY,
        "PathDescriptionCache: updated data for path " << msg->Path);
}

void TPathDescriptionCache::HandleSyncPathDescriptionCache(
    const TEvSSProxy::TEvSyncPathDescriptionCacheRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvSSProxy::TEvSyncPathDescriptionCacheResponse;

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

STFUNC(TPathDescriptionCache::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvSSProxyPrivate::TEvReadPathDescriptionCacheRequest, HandleReadPathDescriptionCache);
        HFunc(TEvSSProxyPrivate::TEvUpdatePathDescriptionCacheRequest, HandleUpdatePathDescriptionCache);
        HFunc(TEvSSProxy::TEvSyncPathDescriptionCacheRequest, HandleSyncPathDescriptionCache);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::SS_PROXY);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
