#pragma once

#include "checksum_range.h"
#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

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
    enum class EStatus
    {
        Unknown,
        Ok,
        MinorMismatch,
        MajorMismatch,
    };
    const TRequestInfoPtr RequestInfo;
    const ui32 BlockSize;
    const TBlockRange64 Range;
    const TVector<TReplicaDescriptor> Replicas;
    const TString WriterClientId;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;

    EStatus Status = EStatus::Unknown;
    TVector<size_t> ActorsToResync;
    ui32 ResyncedCount = 0;
    TVector<NProto::TIOVector> ReadBuffers;
    NProto::TError Error;

    TInstant ReadStartTs;
    TDuration ReadDuration;
    TInstant WriteStartTs;
    TDuration WriteDuration;
    TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;

    TChecksumRangeActorCompanion ChecksumRangeActorCompanion{Replicas};

public:
    TResyncRangeActor(
        TRequestInfoPtr requestInfo,
        ui32 blockSize,
        TBlockRange64 range,
        TVector<TReplicaDescriptor> replicas,
        TString writerClientId,
        IBlockDigestGeneratorPtr blockDigestGenerator);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void CompareChecksums(const NActors::TActorContext& ctx);
    void ResyncMinor(
        const NActors::TActorContext& ctx,
        const TVector<ui64>& checksums,
        ui64 majorChecksum);
    void ResyncMajor(
        const NActors::TActorContext& ctx,
        const TVector<ui64>& checksums);
    void PrepareWriteBuffer(const NActors::TActorContext& ctx);
    void ReadBlocks(const NActors::TActorContext& ctx, size_t idx);
    void WriteBlocks(const NActors::TActorContext& ctx);
    void WriteReplicaBlocks(
        const NActors::TActorContext& ctx,
        size_t idx,
        NProto::TIOVector data);
    void Done(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleChecksumResponse(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChecksumUndelivery(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
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

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
