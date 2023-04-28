#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/ss_proxy/protos/path_description_cache.pb.h>
#include <cloud/blockstore/libs/storage/ss_proxy/ss_proxy_events_private.h>
#include <cloud/storage/core/libs/kikimr/public.h>

#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>

#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/file.h>
#include <util/system/file_lock.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TPathDescriptionCache final
    : public NActors::TActorBootstrapped<TPathDescriptionCache>
{
private:
    const TFsPath CacheFilePath;
    IFileIOServicePtr FileIOService;
    const bool SyncEnabled = false;

    NSSProxy::NProto::TPathDescriptionCache Cache;

    const TFsPath TmpCacheFilePath;
    std::unique_ptr<TFileHandle> TmpCacheFileHandle;
    std::unique_ptr<TFileLock> TmpCacheFileLock;
    TString TmpCacheFileBuffer;

    bool SyncInProgress = false;
    TVector<TRequestInfoPtr> SyncRequests;

public:
    // Never reads from cache file when sync is enabled, just overwrites its
    // content.
    TPathDescriptionCache(
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

    void HandleReadPathDescriptionCache(
        const TEvSSProxyPrivate::TEvReadPathDescriptionCacheRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdatePathDescriptionCache(
        const TEvSSProxyPrivate::TEvUpdatePathDescriptionCacheRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleSyncPathDescriptionCache(
        const TEvSSProxy::TEvSyncPathDescriptionCacheRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
