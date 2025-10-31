#pragma once

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/disk_agent/disk_agent_private.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Sends a write request to the disk-agents specified in ReplicationTargets.
// Then wait for all writes to be completed and send a response.
// The response can be sent in two ways. If the client provided the
// responsePromise, the response will be transmitted using that. Otherwise,
// TEvWriteDeviceBlocksResponse will be sent through the actor system.
class TMultiAgentWriteDeviceBlocksActor final
    : public NActors::TActorBootstrapped<TMultiAgentWriteDeviceBlocksActor>
{
public:
    using TMultiAgentWriteDeviceBlocksResponse =
        TEvDiskAgentPrivate::TMultiAgentWriteDeviceBlocksResponse;
    using TResponsePromise =
        NThreading::TPromise<TMultiAgentWriteDeviceBlocksResponse>;

private:
    const NActors::TActorId Parent;
    const TRequestInfoPtr RequestInfo;
    const TDuration MaxRequestTimeout;

    NProto::TWriteDeviceBlocksRequest Request;
    TResponsePromise ResponsePromise;
    TVector<std::optional<NProto::TError>> Responses;

public:
    TMultiAgentWriteDeviceBlocksActor(
        const NActors::TActorId& parent,
        TRequestInfoPtr requestInfo,
        NProto::TWriteDeviceBlocksRequest request,
        TResponsePromise responsePromise,
        TDuration maxRequestTimeout);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    bool AllResponsesHaveBeenReceived() const;

    void ReplyAndDie(const NActors::TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleWriteBlocksUndelivery(
        const TEvDiskAgent::TEvWriteDeviceBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleTimeout(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlocksResponse(
        const TEvDiskAgent::TEvWriteDeviceBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
