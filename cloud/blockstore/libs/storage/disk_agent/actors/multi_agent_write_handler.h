#pragma once

#include <cloud/blockstore/libs/storage/disk_agent/disk_agent_private.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// IMultiAgentWriteHandler interface defines a method to perform write blocks
// using multiple agents, returning a future with the response.
class IMultiAgentWriteHandler
{
public:
    virtual ~IMultiAgentWriteHandler() = default;

    virtual NThreading::TFuture<
        TEvDiskAgentPrivate::TMultiAgentWriteDeviceBlocksResponse>
    PerformMultiAgentWrite(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDeviceBlocksRequest> request) = 0;
};

using IMultiAgentWriteHandlerPtr = std::shared_ptr<IMultiAgentWriteHandler>;

IMultiAgentWriteHandlerPtr CreateMultiAgentWriteHandler(
    NActors::TActorSystem* actorSystem,
    NActors::TActorId diskAgentId);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
