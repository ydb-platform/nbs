#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/events.h>
#include <cloud/filestore/libs/storage/core/model.h>
#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <contrib/ydb/library/actors/core/event_local.h>

#include <util/generic/string.h>

#include <optional>

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
    // Multi-shard filesystem create and resize steps
    //

    struct TMultiShardDescribeResponse
    {
        TMultiShardFileStoreConfig Config;
        NProtoPrivate::TGetFileSystemTopologyResponse Topology;
        std::optional<NProtoPrivate::TFileSystemShardCreationState> ShardCreationState;
    };

    struct TMultiShardCreateResponse
    {
        NProtoPrivate::TShardTopology Topology;
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
        EvStartReadData,
        EvReleaseReadDataActor,
        EvFastShardsDeallocated,
        EvStartWriteData,
        EvReleaseWriteDataActor,

        EvMultiShardDescribeResponse,
        EvMultiShardAlterResponse,
        EvMultiShardCreateResponse,
        EvMultiShardConfigureResponse,

        EvEnd
    };

    static_assert(EvEnd < (int)TFileStoreEventsPrivate::SERVICE_WORKER_END,
        "EvEnd expected to be < TFileStoreEventsPrivate::SERVICE_WORKER_END");

    using TEvPingSession = TRequestEvent<TEmpty, EvPingSession>;
    using TEvCreateSession = TRequestEvent<TCreateSession, EvCreateSession>;
    using TEvSessionCreated = TResponseEvent<TSessionCreated, EvSessionCreated>;
    using TEvSessionDestroyed = TResponseEvent<TSessionDestroyed, EvSessionDestroyed>;
    using TEvUpdateStats = TRequestEvent<TEmpty, EvUpdateStats>;
    using TEvFastShardsDeallocated =
        TResponseEvent<TEmpty, EvFastShardsDeallocated>;

    // Unlike TRequestEvent following events dont allocalte TCallContext on
    // the heap
    struct TEvReleaseReadDataActor final
        : public NActors::
              TEventLocal<TEvReleaseReadDataActor, EvReleaseReadDataActor>
    {
    };
    struct TEvReleaseWriteDataActor final
        : public NActors::
              TEventLocal<TEvReleaseWriteDataActor, EvReleaseWriteDataActor>
    {
    };

    using TEvMultiShardDescribeResponse = TResponseEvent<TMultiShardDescribeResponse, EvMultiShardDescribeResponse>;
    using TEvMultiShardAlterResponse = TResponseEvent<TEmpty, EvMultiShardAlterResponse>;
    using TEvMultiShardCreateResponse = TResponseEvent<TMultiShardCreateResponse, EvMultiShardCreateResponse>;
    using TEvMultiShardConfigureResponse = TResponseEvent<TEmpty, EvMultiShardConfigureResponse>;
};

}   // namespace NCloud::NFileStore::NStorage
