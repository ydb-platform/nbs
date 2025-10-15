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
//
// Examples:
//
// 1. No wrappers (DiskRegistry-based)
// Stack:
//   [TNonreplicatedPartitionActor]     (owned by TVolumeActor)
//
//
// 2. No wrappers (BlobStorage-based)
// Stack:
//   [TPartitionActor]                  (owned by bootstraper)
//
//
// 3. Shadow Disk (make sense only for DiskRegistry-based)
// Stack:
//   [TShadowDiskActor] (wrapper)       (owned by TVolumeActor)
//   [TNonreplicatedPartitionActor]     (owned by TShadowDiskActor)
//
//
// 4. Linked Disk (DiskRegistry-based)
// Stack:
//   [TFollowerDiskActor] (wrapper)     (owned by TVolumeActor)
//   [TNonreplicatedPartitionActor]     (owned by TFollowerDiskActor)
//
//
// 5. Linked Disk for Shadow disk  (DiskRegistry-based)
// Stack:
//   [TFollowerDiskActor] (wrapper)     (owned by TVolumeActor)
//   [TShadowDiskActor]   (wrapper)     (owned by TFollowerDiskActor)
//   [TNonreplicatedPartitionActor]     (owned by TShadowDiskActor)
//
//
// 6. Linked Disk (BlobStorage-based)
// Stack:
//   [TFollowerDiskActor] (wrapper)     (owned by TVolumeActor)
//   [TPartitionActor]                  (owned by bootstraper)


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
    const TInstant StartTime = TInstant::Now();

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
    [[nodiscard]] TInstant GetStartTime() const;
};

using TPartitionInfoList = TDeque<TPartitionInfo>;

}   // namespace NCloud::NBlockStore::NStorage
