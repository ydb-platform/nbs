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

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReadBlocksRemoteRequestActor final
    : public TActorBootstrapped<TReadBlocksRemoteRequestActor>
{
private:
    const TEvService::TEvReadBlocksLocalRequest::TPtr Request;
    const ui64 BlockSize;
    const TActorId VolumeClient;

public:
    TReadBlocksRemoteRequestActor(
            TEvService::TEvReadBlocksLocalRequest::TPtr request,
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

        LOG_TRACE(ctx, TBlockStoreComponents::SERVICE,
            "Converted Local to gRPC ReadBlocks request for client %s to volume %s",
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

    std::unique_ptr<TEvService::TEvReadBlocksRequest> CreateRemoteRequest()
    {
        const auto* msg = Request->Get();

        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>(
            msg->CallContext,
            msg->Record);

        // TODO: CommitId
        return request;
    }

private:
    STFUNC(StateWait)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvService::TEvReadBlocksResponse, HandleReadBlocksResponse);

            HFunc(TEvService::TEvReadBlocksRequest, HandleUndelivery);

            default:
                HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
                break;
        }
    }

    void HandleUndelivery(
        const TEvService::TEvReadBlocksRequest::TPtr&,
        const TActorContext& ctx)
    {
        auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(
            MakeTabletIsDeadError(E_REJECTED, __LOCATION__));

        NCloud::Reply(ctx, *Request, std::move(response));
        Die(ctx);
    }

    void HandleReadBlocksResponse(
        const TEvService::TEvReadBlocksResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        auto error = msg->GetError();

        if (SUCCEEDED(error.GetCode())) {
            auto sgListOrError = GetSgList(msg->Record, BlockSize);

            if (HasError(sgListOrError)) {
                error = sgListOrError.GetError();
            } else if (auto guard = Request->Get()->Record.Sglist.Acquire()) {
                const auto& src = sgListOrError.GetResult();
                size_t bytesCopied = SgListCopy(src, guard.Get());
                Y_ABORT_UNLESS(bytesCopied == Request->Get()->Record.GetBlocksCount() * BlockSize);
            } else {
                error.SetCode(E_REJECTED);
                error.SetMessage("failed to fill output buffer");
            }
        }

        LOG_TRACE(ctx, TBlockStoreComponents::SERVICE,
            "Converted ReadBlocks response using gRPC IPC to Local IPC");

        msg->Record.ClearBlocks();
        auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(
            NProto::TReadBlocksLocalResponse(msg->Record));

        NCloud::Reply(ctx, *Request, std::move(response));

        Die(ctx);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateReadBlocksRemoteActor(
    TEvService::TEvReadBlocksLocalRequest::TPtr request,
    ui64 blockSize,
    TActorId volumeClient)
{
    return std::make_unique<TReadBlocksRemoteRequestActor>(
        std::move(request),
        blockSize,
        volumeClient);
}

}   // namespace NCloud::NBlockStore::NStorage
