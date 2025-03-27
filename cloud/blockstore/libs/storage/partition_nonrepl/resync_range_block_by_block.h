#pragma once

#include "part_mirror_resync_util.h"

#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/checksum_range.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TResyncRangeBlockByBlockActor final
    : public NActors::TActorBootstrapped<TResyncRangeBlockByBlockActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const ui32 BlockSize;
    const TBlockRange64 Range;
    const TVector<TReplicaDescriptor> Replicas;
    const TString WriterClientId;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const NProto::EResyncPolicy ResyncPolicy;
    const bool PerformChecksumPreliminaryCheck;

    TVector<size_t> ActorsToResync;
    ui32 ResyncedCount = 0;
    TVector<NProto::TIOVector> ReadBuffers;
    NProto::TError Error;

    TInstant ReadStartTs;
    TDuration ReadDuration;
    TInstant WriteStartTs;
    TDuration WriteDuration;
    TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;
    size_t FixedMinorErrorCount = 0;
    size_t FixedMajorErrorCount = 0;
    size_t FoundMajorErrorCount = 0;

    TChecksumRangeActorCompanion ChecksumRangeActorCompanion{Replicas};

public:
    TResyncRangeBlockByBlockActor(
        TRequestInfoPtr requestInfo,
        ui32 blockSize,
        TBlockRange64 range,
        TVector<TReplicaDescriptor> replicas,
        TString writerClientId,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        NProto::EResyncPolicy resyncPolicy,
        bool performChecksumPreliminaryCheck);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void CompareChecksums(const NActors::TActorContext& ctx);
    void PrepareWriteBuffers(const NActors::TActorContext& ctx);
    void ReadBlocks(const NActors::TActorContext& ctx);
    void ReadReplicaBlocks(
        const NActors::TActorContext& ctx,
        size_t replicaIndex);
    void WriteBlocks(const NActors::TActorContext& ctx);
    void WriteReplicaBlocks(
        const NActors::TActorContext& ctx,
        size_t replicaIndex,
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
