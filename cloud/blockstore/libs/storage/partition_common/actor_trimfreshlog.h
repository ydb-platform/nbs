#pragma once

#include "events_private.h"

#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TTrimFreshLogActor final
    : public NActors::TActorBootstrapped<TTrimFreshLogActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    const NActors::TActorId PartitionActorId;
    const NKikimr::TTabletStorageInfoPtr TabletInfo;
    const ui64 TrimFreshLogToCommitId;
    const ui32 RecordGeneration;
    const ui32 PerGenerationCounter;
    const TVector<ui32> FreshChannels;

    ui32 RequestsInFlight = 0;
    NProto::TError Error;

public:
    TTrimFreshLogActor(
        TRequestInfoPtr requestInfo,
        const NActors::TActorId& partitionActorId,
        NKikimr::TTabletStorageInfoPtr tabletInfo,
        ui64 trimFreshLogToCommitId,
        ui32 recordGeneration,
        ui32 perGenerationCounter,
        TVector<ui32> freshChannels);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void TrimFreshLog(const NActors::TActorContext& ctx);

    void NotifyCompleted(const NActors::TActorContext& ctx);
    void ReplyAndDie(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleCollectGarbageResult(
        const NKikimr::TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
