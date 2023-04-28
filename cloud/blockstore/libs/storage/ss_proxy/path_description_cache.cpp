#include "path_description_cache.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>

#include <cloud/storage/core/libs/common/file_io_service.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration SyncInterval = TDuration::Seconds(10);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TPathDescriptionCache::TPathDescriptionCache(
        TString cacheFilePath,
        IFileIOServicePtr fileIO,
        bool syncEnabled)
    : CacheFilePath(std::move(cacheFilePath))
    , FileIOService(std::move(fileIO))
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

void TPathDescriptionCache::Sync(const TActorContext& ctx)
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
        TmpCacheFileBuffer
    );

    auto* actorSystem = ctx.ActorSystem();
    auto actorID = ctx.SelfID;

    result.Subscribe([=](auto future) {
        auto statusCode = S_OK;

        try {
            future.GetValue();
        } catch (...) {
            LOG_ERROR_S(*actorSystem, TBlockStoreComponents::SS_PROXY,
                "PathDescriptionCache: async write failed with exception: "
                << CurrentExceptionMessage());
            ReportPathDescriptionCacheSyncFailure();

            statusCode = E_IO;
        }

        actorSystem->Send(new IEventHandle(
            actorID,
            actorID,
            new TEvents::TEvCompleted(0, statusCode)
        ));
    });
}

void TPathDescriptionCache::SyncCompleted(
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

    TmpCacheFileHandle.reset();
    TmpCacheFileLock.reset();
    TmpCacheFileBuffer.clear();

    for (auto& requestInfo : SyncRequests) {
        auto response =
            std::make_unique<TEvSSProxy::TEvSyncPathDescriptionCacheResponse>(
                error);
        BLOCKSTORE_TRACE_SENT(ctx, &requestInfo->TraceId, this, response);
        NCloud::Reply(ctx, *requestInfo, std::move(response));
    }
    SyncRequests.clear();

    ScheduleSync(ctx);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::SS_PROXY,
        "PathDescriptionCache: sync completed");
}

////////////////////////////////////////////////////////////////////////////////

void TPathDescriptionCache::HandleCompleted(
    const TEvents::TEvCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    SyncCompleted(ctx, MakeError(ev->Get()->Status));
}

void TPathDescriptionCache::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Sync(ctx);
}

void TPathDescriptionCache::HandleReadPathDescriptionCache(
    const TEvSSProxyPrivate::TEvReadPathDescriptionCacheRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvSSProxyPrivate::TEvReadPathDescriptionCacheResponse;

    auto* msg = ev->Get();
    BLOCKSTORE_TRACE_RECEIVED(ctx, &ev->TraceId, this, msg);

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

    BLOCKSTORE_TRACE_SENT(ctx, &ev->TraceId, this, response);
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

    auto* msg = ev->Get();
    BLOCKSTORE_TRACE_RECEIVED(ctx, &ev->TraceId, this, msg);

    if (SyncEnabled) {
        auto requestInfo = CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            msg->CallContext,
            std::move(ev->TraceId)
        );
        SyncRequests.push_back(std::move(requestInfo));

        Sync(ctx);
        return;
    }

    auto error = MakeError(E_PRECONDITION_FAILED, "sync is disabled");
    auto response = std::make_unique<TResponse>(std::move(error));

    BLOCKSTORE_TRACE_SENT(ctx, &ev->TraceId, this, response);
    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TPathDescriptionCache::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvCompleted, HandleCompleted);
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
