#include "multi_agent_write_handler.h"

#include <cloud/blockstore/libs/storage/disk_agent/disk_agent_private.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TMultiAgentWriteHandler final: public IMultiAgentWriteHandler
{
private:
    NActors::TActorSystem* const ActorSystem;
    const NActors::TActorId DiskAgentId;

public:
    TMultiAgentWriteHandler(
        NActors::TActorSystem* actorSystem,
        NActors::TActorId diskAgentId);

    NThreading::TFuture<TMultiAgentWriteResponsePrivate> PerformMultiAgentWrite(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDeviceBlocksRequest> request) override;
};

TMultiAgentWriteHandler::TMultiAgentWriteHandler(
    NActors::TActorSystem* actorSystem,
    NActors::TActorId diskAgentId)
    : ActorSystem(actorSystem)
    , DiskAgentId(diskAgentId)
{}

NThreading::TFuture<TMultiAgentWriteResponsePrivate>
TMultiAgentWriteHandler::PerformMultiAgentWrite(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteDeviceBlocksRequest> request)
{
    auto req = std::make_unique<
        TEvDiskAgentPrivate::TEvMultiAgentWriteDeviceBlocksRequest>();
    req->Record.Swap(request.get());
    req->ResponsePromise =
        NThreading::NewPromise<TMultiAgentWriteResponsePrivate>();
    req->CallContext = std::move(callContext);

    auto future = req->ResponsePromise.GetFuture();

    auto newEv = std::make_unique<NActors::IEventHandle>(
        DiskAgentId,
        NActors::TActorId(),
        req.release());

    ActorSystem->Send(newEv.release());

    return future;
}

////////////////////////////////////////////////////////////////////////////////

IMultiAgentWriteHandlerPtr CreateMultiAgentWriteHandler(
    NActors::TActorSystem* actorSystem,
    NActors::TActorId diskAgentId)
{
    return std::make_shared<TMultiAgentWriteHandler>(actorSystem, diskAgentId);
}

}   // namespace NCloud::NBlockStore::NStorage
