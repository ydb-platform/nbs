#pragma once

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Sends a write request to the disk-agents specified in AdditionalTargets and
// this disk-agent (parent actor). Then wait for all writes to be completed and
// send a response.
class TMultiAgentWriteBlocksActor final
    : public NActors::TActorBootstrapped<TMultiAgentWriteBlocksActor>
{
private:
    const NActors::TActorId Parent;
    const TRequestInfoPtr RequestInfo;

    NProto::TWriteDeviceBlocksRequest Request;
    TVector<std::optional<NProto::TError>> Responses;

public:
    TMultiAgentWriteBlocksActor(
        const NActors::TActorId& parent,
        TRequestInfoPtr requestInfo,
        NProto::TWriteDeviceBlocksRequest request);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    bool HandleError(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

    void ReplyAndDie(const NActors::TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleWriteBlocksUndelivery(
        const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlocksResponse(
        const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
