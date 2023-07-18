#include "tablet_boot_info_cache.h"

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/kikimr/components.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>

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
        IFileIOServicePtr fileIO,
        bool syncEnabled)
    : LogComponent(logComponent)
    , CacheFilePath(std::move(cacheFilePath))
    , FileIOService(std::move(fileIO))
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

void TTabletBootInfoCache::Sync(const TActorContext& ctx)
{
    if (SyncInProgress) {
        return;
    }

    try {
        auto flags =
            EOpenModeFlag::CreateAlways |
            EOpenModeFlag::WrOnly |
            EOpenModeFlag::Seq;
        TmpCacheFileHandle = std::make_unique<TFileHandle>(
            TmpCacheFilePath, flags);

        TmpCacheFileLock = std::make_unique<TFileLock>(TmpCacheFilePath);
        if (!TmpCacheFileLock->TryAcquire()) {
            auto errorMessage = TStringBuilder()
                << "failed to acquire lock on file: " << TmpCacheFilePath;
            SyncCompleted(ctx, MakeError(E_IO, std::move(errorMessage)));
            return;
        }

        TStringStream ss;
        SerializeToTextFormat(Cache, ss);
        TmpCacheFileBuffer = ss.Str();
    } catch (...) {
        SyncCompleted(ctx, MakeError(E_FAIL, CurrentExceptionMessage()));
        return;
    }

    SyncInProgress = true;

    auto result = FileIOService->AsyncWrite(
        *TmpCacheFileHandle,
        0,  // offset
        TmpCacheFileBuffer);

    auto logComponent = LogComponent;
    auto* actorSystem = ctx.ActorSystem();
    auto actorID = ctx.SelfID;
    auto size = TmpCacheFileBuffer.size();

    result.Subscribe([=](auto future) {
        auto statusCode = S_OK;

        try {
            const ui32 written = future.GetValue();
            Y_VERIFY_DEBUG(written == size);    // TODO
        } catch (...) {
            LOG_ERROR_S(*actorSystem, logComponent,
                "TabletBootInfoCache: async write failed with exception: "
                << CurrentExceptionMessage());
            ReportTabletBootInfoCacheSyncFailure();

            statusCode = E_IO;
        }

        actorSystem->Send(new IEventHandle(
            actorID,
            actorID,
            new TEvents::TEvCompleted(0, statusCode)
        ));
    });
}

void TTabletBootInfoCache::SyncCompleted(
    const TActorContext& ctx,
    NProto::TError error)
{
    SyncInProgress = false;

    if (SUCCEEDED(error.GetCode())) {
        try {
            TmpCacheFilePath.RenameTo(CacheFilePath);
        } catch (...) {
            error = MakeError(E_FAIL, CurrentExceptionMessage());
        }
    }

    if (HasError(error)) {
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

    TmpCacheFileHandle.reset();
    TmpCacheFileLock.reset();
    TmpCacheFileBuffer.clear();

    if (!SyncRequests.empty()) {
        auto requestInfo = std::move(SyncRequests.front());
        auto response =
            std::make_unique<TEvHiveProxy::TEvSyncTabletBootInfoCacheResponse>(
                error);
        NCloud::Reply(ctx, requestInfo, std::move(response));
        SyncRequests.pop();
    }

    ScheduleSync(ctx);

    LOG_DEBUG_S(ctx, LogComponent, "TabletBootInfoCache: sync completed");
}

////////////////////////////////////////////////////////////////////////////////

void TTabletBootInfoCache::HandleCompleted(
    const TEvents::TEvCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    SyncCompleted(ctx, MakeError(ev->Get()->Status));
}

void TTabletBootInfoCache::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Sync(ctx);
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

    if (SyncEnabled) {
        SyncRequests.emplace(ev->Sender, ev->Cookie);
        Sync(ctx);
        return;
    }

    auto error = MakeError(E_PRECONDITION_FAILED, "sync is disabled");
    auto response = std::make_unique<TResponse>(std::move(error));

    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TTabletBootInfoCache::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvCompleted, HandleCompleted);
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
