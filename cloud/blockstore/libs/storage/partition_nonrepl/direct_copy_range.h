#pragma once

#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// TDirectCopyRangeActor copies the range using TEvDirectCopyBlocksRequest. If
// copying is not possible, it performcopying via TCopyRangeActor.
class TDirectCopyRangeActor final
    : public NActors::TActorBootstrapped<TDirectCopyRangeActor>
{
private:
    using TDeviceInfoResponse = std::unique_ptr<
        TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse>;

    const ui32 BlockSize;
    const TBlockRange64 Range;
    const NActors::TActorId SourceActor;
    const NActors::TActorId TargetActor;
    const TString WriterClientId;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;

    TRequestInfoPtr RequestInfo;
    TInstant StartTs;
    TDuration ReadDuration;
    TDuration WriteDuration;
    bool AllZeroes = false;

    TDeviceInfoResponse SourceInfo;
    TDeviceInfoResponse TargetInfo;

public:
    TDirectCopyRangeActor(
        TRequestInfoPtr requestInfo,
        ui32 blockSize,
        TBlockRange64 range,
        NActors::TActorId source,
        NActors::TActorId target,
        TString writerClientId,
        IBlockDigestGeneratorPtr blockDigestGenerator);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void GetDevicesInfo(const NActors::TActorContext& ctx);
    void DirectCopy(const NActors::TActorContext& ctx);
    void Fallback(const NActors::TActorContext& ctx);

    void Done(const NActors::TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleGetDeviceForRange(
        const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse::TPtr&
            ev,
        const NActors::TActorContext& ctx);

    void HandleDirectCopyUndelivered(
        const TEvDiskAgent::TEvDirectCopyBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDirectCopyBlocksResponse(
        const TEvDiskAgent::TEvDirectCopyBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRangeMigrationTimeout(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
