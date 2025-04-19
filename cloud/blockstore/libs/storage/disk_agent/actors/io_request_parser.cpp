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
    TStorageBufferAllocator Allocator;

public:
    TIORequestParserActor(
            const TActorId& owner,
            TStorageBufferAllocator allocator)
        : TActor(&TIORequestParserActor::StateWork)
        , Owner(owner)
        , Allocator(std::move(allocator))
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
        auto request = std::make_unique<
            TEvDiskAgentPrivate::TEvParsedWriteDeviceBlocksRequest>();

        // parse protobuf
        auto* msg = ev->Get<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
        request->Record.Swap(&msg->Record);

        const bool canUseAllocator =
            Allocator &&
            // Don't reallocate if we have to send the request to other agents.
            request->Record.GetAdditionalTargets().empty();

        if (canUseAllocator) {
            const auto& buffers = request->Record.GetBlocks().GetBuffers();

            ui64 bytesCount = 0;
            for (const auto& buffer: buffers) {
                bytesCount += buffer.size();
            }

            request->Storage = Allocator(bytesCount);
            request->StorageSize = bytesCount;

            char* dst = request->Storage.get();
            for (const auto& buffer: buffers) {
                std::memcpy(dst, buffer.data(), buffer.size());
                dst += buffer.size();
            }
            request->Record.ClearBlocks();
        }

        auto newEv = std::make_unique<IEventHandle>(
            ev->Recipient,
            ev->Sender,
            request.release(),
            ev->Flags,
            ev->Cookie,
            nullptr,    // forwardOnNondelivery
            std::move(ev->TraceId));

        newEv->Rewrite(newEv->Type, Owner);

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
    TStorageBufferAllocator allocator)
{
    return std::make_unique<TIORequestParserActor>(owner, std::move(allocator));
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
