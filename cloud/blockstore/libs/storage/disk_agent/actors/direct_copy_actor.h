#pragma once

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Reads a data range from the Source actor and writes it to the target Disk
// Agent and device specified in the Request.
// Then reply to caller with TEvDirectCopyBlocksResponse message.
class TDirectCopyActor final
    : public NActors::TActorBootstrapped<TDirectCopyActor>
{
private:
    const NActors::TActorId Source;
    const TRequestInfoPtr RequestInfo;
    const NProto::TDirectCopyBlocksRequest Request;
    const ui64 RecommendedBandwidth;

    bool AllZeroes = false;
    TInstant ReadStartAt;
    TInstant WriteStartAt;

public:
    TDirectCopyActor(
        const NActors::TActorId& source,
        TRequestInfoPtr requestInfo,
        NProto::TDirectCopyBlocksRequest request,
        ui64 recommendedBandwidth);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    bool HandleError(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

    void Done(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleReadBlocksResponse(
        const TEvDiskAgent::TEvReadDeviceBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlocksUndelivery(
        const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlocksResponse(
        const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleZeroBlocksUndelivery(
        const TEvDiskAgent::TEvZeroDeviceBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleZeroBlocksResponse(
        const TEvDiskAgent::TEvZeroDeviceBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
