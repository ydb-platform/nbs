#include "service_actor.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWriteBlocksRemoteRequestActor final
    : public TActorBootstrapped<TWriteBlocksRemoteRequestActor>
{
private:
    const TEvService::TEvWriteBlocksLocalRequest::TPtr Request;
    const ui64 BlockSize;
    const TActorId VolumeClient;

public:
    TWriteBlocksRemoteRequestActor(
            TEvService::TEvWriteBlocksLocalRequest::TPtr request,
            ui64 blockSize,
            TActorId volumeClient)
        : Request(request)
        , BlockSize(blockSize)
        , VolumeClient(volumeClient)
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        ConvertToRemoteAndSend(ctx);
        Become(&TThis::StateWait);
    }

private:
    void ConvertToRemoteAndSend(const TActorContext& ctx)
    {
        const auto* msg = Request->Get();
        const auto& clientId = GetClientId(*msg);
        const auto& diskId = GetDiskId(*msg);

        auto request = CreateRemoteRequest();
        if (!request) {
            ReplyAndDie(ctx, MakeError(E_REJECTED, "failed to create remote request"));
            return;
        }

        LOG_TRACE(ctx, TBlockStoreComponents::SERVICE,
            "Converted Local to gRPC WriteBlocks request for client %s to volume %s",
            clientId.Quote().data(),
            diskId.Quote().data());

        auto undeliveredActor = SelfId();

        auto event = std::make_unique<IEventHandle>(
            VolumeClient,
            SelfId(),
            request.release(),
            Request->Flags | IEventHandle::FlagForwardOnNondelivery,  // flags
            Request->Cookie,  // cookie
            &undeliveredActor    // forwardOnNondelivery
        );
        ctx.Send(event.release());
    }

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateRemoteRequest()
    {
        const auto* msg = Request->Get();

        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>(
            msg->CallContext,
            msg->Record);

        auto dst = ResizeIOVector(
            *request->Record.MutableBlocks(),
            msg->Record.BlocksCount,
            BlockSize);

        if (auto guard = msg->Record.Sglist.Acquire()) {
            size_t bytesWritten = SgListCopy(guard.Get(), dst);
            Y_ABORT_UNLESS(bytesWritten == msg->Record.BlocksCount * BlockSize);
            return request;
        }

        return nullptr;
    }

    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error)
    {
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(error);

        NCloud::Reply(ctx, *Request, std::move(response));

        Die(ctx);
    }

private:
    STFUNC(StateWait)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvService::TEvWriteBlocksResponse, HandleWriteBlocksResponse);

            HFunc(TEvService::TEvWriteBlocksRequest, HandleUndelivery);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::SERVICE,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandleUndelivery(
        const TEvService::TEvWriteBlocksRequest::TPtr&,
        const TActorContext& ctx)
    {
        ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
    }

    void HandleWriteBlocksResponse(
        const TEvService::TEvWriteBlocksResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
            msg->Record);

        NCloud::Reply(ctx, *Request, std::move(response));

        Die(ctx);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateWriteBlocksRemoteActor(
    TEvService::TEvWriteBlocksLocalRequest::TPtr request,
    ui64 blockSize,
    TActorId volumeClient)
{
    return std::make_unique<TWriteBlocksRemoteRequestActor>(
        std::move(request),
        blockSize,
        volumeClient);
}

}   // namespace NCloud::NBlockStore::NStorage
