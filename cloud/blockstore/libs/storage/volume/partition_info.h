#pragma once

#include <cloud/blockstore/libs/storage/protos/part.pb.h>
#include <cloud/blockstore/libs/storage/volume/model/retry_policy.h>

#include <cloud/storage/core/libs/kikimr/public.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/scheduler_cookie.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/stack.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Used to store the partition-related actors, the actor of the partitions
// itself and its wrappers.
class TActorsStack
{
public:
    enum class EActorPurpose
    {
        BlobStoragePartitionTablet,
        DiskRegistryBasedPartitionActor,
        FollowerWrapper,
        ShadowDiskWrapper,
    };

private:
    struct TActorInfo
    {
        TActorInfo(NActors::TActorId actorId, EActorPurpose purpose)
            : ActorId(actorId)
            , ActorPurpose(purpose)
        {}
        NActors::TActorId ActorId;
        EActorPurpose ActorPurpose;
    };

    TDeque<TActorInfo> Actors;

public:
    TActorsStack() = default;
    TActorsStack(NActors::TActorId actor, EActorPurpose purpose);

    void Push(NActors::TActorId actorId, EActorPurpose purpose);
    void Clear();
    [[nodiscard]] bool IsKnown(NActors::TActorId actorId) const;
    [[nodiscard]] NActors::TActorId GetTop() const;
    [[nodiscard]] NActors::TActorId GetTopWrapper() const;
};

////////////////////////////////////////////////////////////////////////////////
struct TPartitionInfo
{
    enum EState
    {
        UNKNOWN,
        STOPPED,
        STARTED,
        READY,
        FAILED,
    };

    const ui64 TabletId;
    const ui32 PartitionIndex;
    const NProto::TPartitionConfig PartitionConfig;

    TRetryPolicy RetryPolicy;
    NActors::TSchedulerCookieHolder RetryCookie;

    bool RequestingBootExternal = false;
    ui32 SuggestedGeneration = 0;
    NKikimr::TTabletStorageInfoPtr StorageInfo;

    NActors::TActorId Bootstrapper;
    TActorsStack RelatedActors;

    EState State = UNKNOWN;
    TString Message;

    TDuration ExternalBootTimeout;

    TPartitionInfo(
        ui64 tabletId,
        NProto::TPartitionConfig partitionConfig,
        ui32 partitionIndex,
        TDuration timeoutIncrement,
        TDuration timeoutMax);

    void Init(const NActors::TActorId& bootstrapper);
    void SetStarted(TActorsStack actors);
    void SetReady();
    void SetStopped();
    void SetFailed(TString message);

    [[nodiscard]] NActors::TActorId GetTopActorId() const;
    [[nodiscard]] bool IsKnownActorId(const NActors::TActorId actorId) const;

    [[nodiscard]] TString GetStatus() const;
};

using TPartitionInfoList = TDeque<TPartitionInfo>;

}   // namespace NCloud::NBlockStore::NStorage
