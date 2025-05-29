#include "multi_agent_write_handler.h"

#include <cloud/blockstore/libs/storage/disk_agent/disk_agent_private.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

class TMultiAgentWriteHandler final: public IMultiAgentWriteHandler
{
public:
    using TMultiAgentWriteDeviceBlocksResponse =
        TEvDiskAgentPrivate::TMultiAgentWriteDeviceBlocksResponse;

private:
    TActorSystem* const ActorSystem;
    const TActorId DiskAgentId;

public:
    TMultiAgentWriteHandler(TActorSystem* actorSystem, TActorId diskAgentId);

    NThreading::TFuture<TMultiAgentWriteDeviceBlocksResponse>
    PerformMultiAgentWrite(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDeviceBlocksRequest> request) override;
};

////////////////////////////////////////////////////////////////////////////////

TMultiAgentWriteHandler::TMultiAgentWriteHandler(
        TActorSystem* actorSystem,
        TActorId diskAgentId)
    : ActorSystem(actorSystem)
    , DiskAgentId(diskAgentId)
{}

NThreading::TFuture<TEvDiskAgentPrivate::TMultiAgentWriteDeviceBlocksResponse>
TMultiAgentWriteHandler::PerformMultiAgentWrite(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteDeviceBlocksRequest> request)
{
    auto req = std::make_unique<
        TEvDiskAgentPrivate::TEvMultiAgentWriteDeviceBlocksRequest>();
    req->Record.Swap(request.get());
    req->ResponsePromise =
        NThreading::NewPromise<TMultiAgentWriteDeviceBlocksResponse>();
    req->CallContext = std::move(callContext);

    auto future = req->ResponsePromise.GetFuture();

    auto newEv =
        std::make_unique<IEventHandle>(DiskAgentId, TActorId(), req.release());

    ActorSystem->Send(newEv.release());

    return future;
}

////////////////////////////////////////////////////////////////////////////////

IMultiAgentWriteHandlerPtr CreateMultiAgentWriteHandler(
    TActorSystem* actorSystem,
    TActorId diskAgentId)
{
    return std::make_shared<TMultiAgentWriteHandler>(actorSystem, diskAgentId);
}

}   // namespace NCloud::NBlockStore::NStorage
