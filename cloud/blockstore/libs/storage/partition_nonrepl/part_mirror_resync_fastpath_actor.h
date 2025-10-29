#pragma once

#include "part_nonrepl_events_private.h"
#include "resync_range.h"   // todo: move TResyncReplica to other place?

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/storage/core/libs/common/error.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////
// Implements read requests for an unsynced mirrored disk.
// Reads from all replicas and returns a successful response if the checksums
// for all replicas are equal.
// Otherwise returns an error - the recipient is supposed to fall back to
// slow path.
class TMirrorPartitionResyncFastPathActor final
    : public NActors::TActorBootstrapped<TMirrorPartitionResyncFastPathActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const ui32 BlockSize;
    const TBlockRange64 Range;
    const TVector<TReplicaDescriptor> Replicas;
    const TString ClientId;
    TGuardedSgList SgList;

    THashMap<int, ui64> Checksums;
    NProto::TError Error;

public:
    TMirrorPartitionResyncFastPathActor(
        TRequestInfoPtr requestInfo,
        ui32 blockSize,
        TBlockRange64 range,
        TGuardedSgList sgList,
        TVector<TReplicaDescriptor> replicas,
        TString clientId);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void ChecksumBlocks(const NActors::TActorContext& ctx);
    void ChecksumReplicaBlocks(const NActors::TActorContext& ctx, int idx);
    void CompareChecksums(const NActors::TActorContext& ctx);
    void CalculateChecksum();
    void ReadBlocks(const NActors::TActorContext& ctx);
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
