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

#include <library/cpp/actors/core/actor_bootstrapped.h>

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
    const ui64 MaxBlocksCount;
    const TActorId VolumeClient;

public:
    TWriteBlocksRemoteRequestActor(
            TEvService::TEvWriteBlocksLocalRequest::TPtr request,
            ui64 blockSize,
            ui64 maxBlocksCount,
            TActorId volumeClient)
        : Request(request)
        , BlockSize(blockSize)
        , MaxBlocksCount(maxBlocksCount)
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

        const ui64 startIndex = msg->Record.GetStartIndex();
        ui32 blocksCount = msg->Record.BlocksCount;

        if (!blocksCount ||
            MaxBlocksCount <= startIndex ||
            MaxBlocksCount - startIndex < blocksCount)
        {
            auto error = MakeError(E_ARGUMENT, TStringBuilder()
                << "invalid block range [index" << startIndex
                << ", count: " << blocksCount << "]");
            ReplyAndDie(ctx, error);
            return;
        }

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
                HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
                break;
        }
    }

    void HandleUndelivery(
        const TEvService::TEvWriteBlocksRequest::TPtr&,
        const TActorContext& ctx)
    {
        ReplyAndDie(ctx, MakeError(E_REJECTED, "Tablet is dead"));
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
    ui64 maxBlocksCount,
    TActorId volumeClient)
{
    return std::make_unique<TWriteBlocksRemoteRequestActor>(
        std::move(request),
        blockSize,
        maxBlocksCount,
        volumeClient);
}

}   // namespace NCloud::NBlockStore::NStorage
