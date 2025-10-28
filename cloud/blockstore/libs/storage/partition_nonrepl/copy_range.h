#pragma once

#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TCopyRangeActor final
    : public NActors::TActorBootstrapped<TCopyRangeActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const ui32 BlockSize;
    const TBlockRange64 Range;
    const NActors::TActorId Source;
    const NActors::TActorId Target;
    const TString WriterClientId;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const NActors::TActorId VolumeActorId;
    const bool AssignVolumeRequestId;
    const NActors::TActorId ActorToLockAndDrainRange;

    ui64 VolumeRequestId = 0;
    TInstant ReadStartTs;
    TDuration ReadDuration;
    TInstant WriteStartTs;
    TDuration WriteDuration;
    TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;
    bool AllZeroes = false;
    bool NeedToReleaseRange = false;

public:
    TCopyRangeActor(
        TRequestInfoPtr requestInfo,
        ui32 blockSize,
        TBlockRange64 range,
        NActors::TActorId source,
        NActors::TActorId target,
        TString writerClientId,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        NActors::TActorId volumeActorId,
        bool assignVolumeRequestId,
        NActors::TActorId actorToLockAndDrainRange);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void GetVolumeRequestId(const NActors::TActorContext& ctx);
    void LockAndDrainRange(const NActors::TActorContext& ctx);
    void ReadBlocks(const NActors::TActorContext& ctx);
    void WriteBlocks(
        const NActors::TActorContext& ctx,
        NProto::TReadBlocksResponse& readResponse);
    void ZeroBlocks(const NActors::TActorContext& ctx);
    void Done(const NActors::TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleVolumeRequestId(
        const TEvVolumePrivate::TEvTakeVolumeRequestIdResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleLockAndDrainRangeResponse(
        const NPartition::TEvPartition::TEvLockAndDrainRangeResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadResponse(
        const TEvService::TEvReadBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadUndelivery(
        const TEvService::TEvReadBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteResponse(
        const TEvService::TEvWriteBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteUndelivery(
        const TEvService::TEvWriteBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleZeroResponse(
        const TEvService::TEvZeroBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleZeroUndelivery(
        const TEvService::TEvZeroBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
