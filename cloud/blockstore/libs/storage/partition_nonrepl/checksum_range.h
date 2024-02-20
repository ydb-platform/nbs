#pragma once

#include "part_nonrepl_events_private.h"
#include "part_mirror_resync_util.h"

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

class TChecksumRangeActor final
    : public NActors::TActorBootstrapped<TChecksumRangeActor>
{
    const TRequestInfoPtr RequestInfo;
    const TBlockRange64 Range;
    const TVector<TReplicaId> Replicas;

    THashMap<int, ui64> Checksums;
    NProto::TError Error;
    TInstant ChecksumStartTs;
    TDuration ChecksumDuration;

public:
    TChecksumRangeActor(
        TRequestInfoPtr requestInfo,
        TBlockRange64 range,
        TVector<TReplicaId> replicas);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void ChecksumBlocks(const NActors::TActorContext& ctx);
    void ChecksumReplicaBlocks(const NActors::TActorContext& ctx, int idx);
    void Done(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleChecksumResponse(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleChecksumUndelivery(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
