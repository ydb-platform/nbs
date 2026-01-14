#pragma once

#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>
#include <cloud/blockstore/libs/storage/volume/actors/multi_partition_requests.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

// Wrapper for multi-partitioned YDB-based disks. Hides the partitioning details
// from volume actor.
// For incoming requests, the wrapper determines whether to send the request to
// one or multiple partitions. If the request needs to be executed in multiple
// partitions, a TMultiPartitionRequestActor is created to handle the request.
// The response is sent directly to the volume actor, bypassing the wrapper.
class TMultiPartitionWrapperActor final
    : public NActors::TActorBootstrapped<TMultiPartitionWrapperActor>
{
private:
    const TChildLogTitle LogTitle;
    const ITraceSerializerPtr TraceSerializer;
    const ui32 BlockSize = 0;
    const ui32 BlocksPerStripe = 0;
    const TBriefPartitionInfoList Partitions;

public:
    explicit TMultiPartitionWrapperActor(
        TChildLogTitle logTitle,
        ITraceSerializerPtr traceSerializer,
        const TString& diskId,
        ui32 blockSize,
        ui32 blocksPerStripe,
        const TVector<NActors::TActorId>& partitionActors);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void StateWork(TAutoPtr<::NActors::IEventHandle>& ev);

    template <typename TMethod>
    void HandleRequest(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
