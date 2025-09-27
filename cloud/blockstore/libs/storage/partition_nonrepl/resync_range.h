#pragma once

#include "checksum_range.h"
#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TResyncRangeActor final
    : public NActors::TActorBootstrapped<TResyncRangeActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const ui32 BlockSize;
    const TBlockRange64 Range;
    const TVector<TReplicaDescriptor> Replicas;
    const TString WriterClientId;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const NProto::EResyncPolicy ResyncPolicy;
    const NActors::TActorId VolumeActorId;
    const bool AssignVolumeRequestId;

    TVector<int> ActorsToResync;
    ui32 ResyncedCount = 0;
    TGuardedBuffer<TString> Buffer;
    TGuardedSgList SgList;
    NProto::TError Error;

    TInstant ReadStartTs;
    TDuration ReadDuration;
    IProfileLog::TRangeInfo ReadRangeInfo{.Range = Range};
    TInstant WriteStartTs;
    TDuration WriteDuration;
    IProfileLog::TRangeInfo WriteRangeInfo{.Range = Range};
    TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;
    ui64 VolumeRequestId = 0;
    int ReplicaIndexToReadFrom = 0;

    TChecksumRangeActorCompanion ChecksumRangeActorCompanion{Replicas};

    bool ErrorFound = false;
    bool ErrorFixed = false;

public:
    TResyncRangeActor(
        TRequestInfoPtr requestInfo,
        ui32 blockSize,
        TBlockRange64 range,
        TVector<TReplicaDescriptor> replicas,
        TString writerClientId,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        NProto::EResyncPolicy resyncPolicy,
        NActors::TActorId volumeActorId,
        bool assignVolumeRequestId);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void GetVolumeRequestId(const NActors::TActorContext& ctx);
    void CompareChecksums(const NActors::TActorContext& ctx);
    void ReadBlocks(const NActors::TActorContext& ctx);
    void WriteBlocks(const NActors::TActorContext& ctx);
    void WriteReplicaBlocks(const NActors::TActorContext& ctx, int idx);
    void Done(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleChecksumResponse(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChecksumUndelivery(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleVolumeRequestId(
        const TEvVolumePrivate::TEvTakeVolumeRequestIdResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadResponse(
        const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadUndelivery(
        const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteResponse(
        const TEvService::TEvWriteBlocksLocalResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteUndelivery(
        const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
