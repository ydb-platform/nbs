#pragma once

#include "events_private.h"

#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/partition_common/model/fresh_blob.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/hash.h>
#include <util/generic/set.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TGroupRange
{
    ui64 FromCommit = 0;
    ui64 ToCommit = 0;
    ui32 GroupId = 0;
};

////////////////////////////////////////////////////////////////////////////////

TVector<TGroupRange> BuildGroupRequestsForChannel(
    const TVector<NKikimr::TTabletChannelInfo::THistoryEntry>& history,
    ui64 tabletId,
    ui64 trimFreshLogToCommitId);

////////////////////////////////////////////////////////////////////////////////

class TLoadFreshBlobsActor final
    : public NActors::TActorBootstrapped<TLoadFreshBlobsActor>
{
private:
    using TBlobIds = TSet<NKikimr::TLogoBlobID>;
    using TGroupIdToBlobIds = THashMap<ui32, TBlobIds>;

private:
    const NActors::TActorId PartitionActorId;
    const NKikimr::TTabletStorageInfoPtr TabletInfo;
    const EStorageAccessMode StorageAccessMode;
    const ui64 TrimFreshLogToCommitId;
    const TVector<ui32> FreshChannels;

    ui32 RangeRequestsInFlight = 0;

    NProto::TError Error;
    TVector<TFreshBlob> Blobs;

public:
    TLoadFreshBlobsActor(
        const NActors::TActorId& partitionActorId,
        NKikimr::TTabletStorageInfoPtr tabletInfo,
        EStorageAccessMode storageAccessMode,
        ui64 trimFreshLogToCommitId,
        TVector<ui32> freshChannels);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void DiscoverBlobs(const NActors::TActorContext& ctx);
    void NotifyAndDie(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRangeResult(
        const NKikimr::TEvBlobStorage::TEvRangeResult::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
