#include "io_request_parser.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/disk_agent/disk_agent_private.h>
#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TIORequestParserActor: public TActor<TIORequestParserActor>
{
private:
    const TActorId Owner;
    TWriteDeviceBlocksRequestParser Parser;

public:
    TIORequestParserActor(
            const TActorId& owner,
            TWriteDeviceBlocksRequestParser parser)
        : TActor(&TIORequestParserActor::StateWork)
        , Owner(owner)
        , Parser(std::move(parser))
    {}

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

            case TEvDiskAgent::EvWriteDeviceBlocksRequest:
                HandleWriteDeviceBlocks(ev);
                break;

            case TEvDiskAgent::EvReadDeviceBlocksRequest:
                HandleRequest<TEvDiskAgent::TEvReadDeviceBlocksRequest>(
                    ev,
                    TEvDiskAgentPrivate::EvParsedReadDeviceBlocksRequest);
                break;

            case TEvDiskAgent::EvZeroDeviceBlocksRequest:
                HandleRequest<TEvDiskAgent::TEvZeroDeviceBlocksRequest>(
                    ev,
                    TEvDiskAgentPrivate::EvParsedZeroDeviceBlocksRequest);
                break;

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_AGENT_WORKER);
                break;
        }
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        Die(ctx);
    }

    void HandleWriteDeviceBlocks(TAutoPtr<IEventHandle>& ev)
    {
        auto eventBase = Parser(ev);

        auto newEv = std::make_unique<IEventHandle>(
            ev->Recipient,
            ev->Sender,
            eventBase.Release(),
            ev->Flags,
            ev->Cookie,
            nullptr,    // forwardOnNondelivery
            std::move(ev->TraceId));

        newEv->Rewrite(
            TEvDiskAgentPrivate::EvParsedWriteDeviceBlocksRequest,
            Owner);

        ActorContext().Send(std::move(newEv));
    }

    template <typename TRequest>
    void HandleRequest(TAutoPtr<IEventHandle>& ev, ui32 typeRewrite)
    {
        // parse protobuf
        const auto* msg = ev->Get<TRequest>();
        Y_UNUSED(msg);

        ev->Rewrite(typeRewrite, Owner);
        ActorContext().Send(ev);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IActor> CreateIORequestParserActor(
    const TActorId& owner,
    TWriteDeviceBlocksRequestParser parser)
{
    return std::make_unique<TIORequestParserActor>(owner, std::move(parser));
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
