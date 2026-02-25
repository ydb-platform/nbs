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

    struct TActorToTrimFreshLogToCommitId {
        NActors::TActorId ActorId;
        ui64 CommitId = 0;
    };

private:
    const TRequestInfoPtr RequestInfo;

    const NActors::TActorId PartitionActorId;
    const NKikimr::TTabletStorageInfoPtr TabletInfo;
    const ui32 RecordGeneration;
    const ui32 PerGenerationCounter;
    const TVector<ui32> FreshChannels;
    const TString DiskId;
    const TDuration Timeout;
    const ui64 LastTrimFreshLogToCommitId;

    TVector<TActorToTrimFreshLogToCommitId> ActorToTrimFreshLogToCommitId;

    ui64 ActorIdx = 0;

    std::optional<ui64> TrimFreshLogToCommitId;

    ui32 RequestsInFlight = 0;
    NProto::TError Error;

public:
    TTrimFreshLogActor(
        TRequestInfoPtr requestInfo,
        const NActors::TActorId& partitionActorId,
        NKikimr::TTabletStorageInfoPtr tabletInfo,
        ui32 recordGeneration,
        ui32 perGenerationCounter,
        TVector<ui32> freshChannels,
        TString diskId,
        TDuration timeout,
        const TVector<NActors::TActorId>& actorsToGetTrimFreshLogToCommitId,
        ui64 lastTrimFreshLogToCommitId);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void GetTrimFreshLogToCommitId(const NActors::TActorContext& ctx);

    void TrimFreshLog(const NActors::TActorContext& ctx);

    void NotifyCompleted(const NActors::TActorContext& ctx);
    void ReplyAndDie(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWaitTrimFreshLogToCommitId);
    STFUNC(StateWork);

    void HandleGetTrimFreshLogToCommitIdResponse(
        const TEvPartitionCommonPrivate::TEvGetTrimFreshLogToCommitIdResponse::
            TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleCollectGarbageResult(
        const NKikimr::TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
