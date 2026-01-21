#pragma once

#include "part_nonrepl_events_private.h"
#include "resync_range.h"   // todo: move TResyncReplica to other place?

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////
// Implements read requests for an unsynced mirrored disk.
// Reads from all replicas and returns a successful response if the checksums
// for all replicas are equal.
// Otherwise returns an error - the recipient is supposed to fall back to
// slow path.
// When "OptimizeFastPathReadsOnResync" is enabled, the actor replies to the
// client after reading data and 1 checksum with "EvResyncFastPathReadResponse".
// After all checksums are read, the actor compares the checksums and replies
// with "EvResyncFastPathChecksumCompareResponse", which should trigger resync
// of the range.
// "EvResyncFastPathChecksumCompareResponse" will only be sent if
// "OptimizeFastPathReadsOnResync" is true and "EvResyncFastPathReadResponse"
// was already sent and did not contain an error.
class TMirrorPartitionResyncFastPathActor final
    : public NActors::TActorBootstrapped<TMirrorPartitionResyncFastPathActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString DiskId;
    const ui32 BlockSize;
    const TBlockRange64 Range;
    const TVector<TReplicaDescriptor> Replicas;
    const TString ClientId;
    const bool OptimizeFastPathReadsOnResync;
    TGuardedSgList SgList;

    std::optional<NProto::TError> CompareChecksumsError;
    THashMap<ui32, ui32> Checksums;
    std::optional<ui32> DataChecksum;
    bool ReplySent = false;

public:
    TMirrorPartitionResyncFastPathActor(
        TRequestInfoPtr requestInfo,
        TString diskId,
        ui32 blockSize,
        TBlockRange64 range,
        TGuardedSgList sgList,
        TVector<TReplicaDescriptor> replicas,
        TString clientId,
        bool optimizeFastPathReadsOnResync);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void ChecksumBlocks(const NActors::TActorContext& ctx);
    void ReadBlocks(const NActors::TActorContext& ctx);
    void ChecksumReplicaBlocks(const NActors::TActorContext& ctx, int idx);
    NProto::TError CompareChecksums(const NActors::TActorContext& ctx) const;
    void MaybeCompareChecksums(const NActors::TActorContext& ctx);
    bool MaybeReplyAndDie(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);
    TResultOrError<ui32> CalculateDataChecksum() const;
    bool AllChecksumsReceived() const;
    void Reply(const NActors::TActorContext& ctx, const NProto::TError& error);
    void FinishResync(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);
    void ReplyAndDie(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleChecksumResponse(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChecksumUndelivery(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadResponse(
        const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadUndelivery(
        const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
