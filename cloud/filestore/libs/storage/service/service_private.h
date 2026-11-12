#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/events.h>
#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

struct TEvServicePrivate
{
    //
    // Session start/stop
    //

    struct TCreateSession
    {
        TString ClientId;
        TString FileSystemId;
        TString CheckpointId;
        TString SessionId;
        bool ReadOnly = false;
        ui64 SessionSeqNo = 0;
        bool RestoreClientSession = false;
        TRequestInfoPtr RequestInfo;
    };

    struct TSessionCreated
    {
        TString ClientId;
        TString FileSystemId;
        TString SessionId;
        bool ReadOnly = false;
        ui64 SessionSeqNo = 0;
        TString SessionState;
        ui64 TabletId = 0;
        ui64 OwnerGeneration = 0;
        NProto::TFileStore FileStore;
        TRequestInfoPtr RequestInfo;
        bool Shutdown = false;
    };

    struct TSessionDestroyed
    {
        TString SessionId;
        ui64 SeqNo;
    };

    //
    // Fast shard device allocation
    //

    struct TAllocatedFastShardConfig
    {
        ui64 TabletId = 0;
        NProtoPrivate::TPersistentFastShardConfig Config;
    };

    struct TFastShardsAllocated
    {
        THashMap<TString, TAllocatedFastShardConfig> FastShards;
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TFileStoreEventsPrivate::SERVICE_WORKER_START,

        EvPingSession,
        EvCreateSession,
        EvSessionCreated,
        EvSessionDestroyed,
        EvUpdateStats,
        EvFastShardsAllocated,
        EvFastShardsDeallocated,

        EvEnd
    };

    static_assert(EvEnd < (int)TFileStoreEventsPrivate::SERVICE_WORKER_END,
        "EvEnd expected to be < TFileStoreEventsPrivate::SERVICE_WORKER_END");

    using TEvPingSession = TRequestEvent<TEmpty, EvPingSession>;
    using TEvCreateSession = TRequestEvent<TCreateSession, EvCreateSession>;
    using TEvSessionCreated = TResponseEvent<TSessionCreated, EvSessionCreated>;
    using TEvSessionDestroyed = TResponseEvent<TSessionDestroyed, EvSessionDestroyed>;
    using TEvUpdateStats = TRequestEvent<TEmpty, EvUpdateStats>;
    using TEvFastShardsAllocated = TResponseEvent<TFastShardsAllocated, EvFastShardsAllocated>;
    using TEvFastShardsDeallocated = TResponseEvent<TEmpty, EvFastShardsDeallocated>;
};

}   // namespace NCloud::NFileStore::NStorage
