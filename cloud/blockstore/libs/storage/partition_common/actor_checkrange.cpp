#include "actor_checkrange.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <util/generic/string.h>
#include <util/stream/str.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TCheckRangeActor::TCheckRangeActor(
    const TActorId& partition,
    NProto::TCheckRangeRequest&& request,
    TRequestInfoPtr&& requestInfo)
    : Partition(partition)
    , Request(std::move(request))
    , RequestInfo(std::move(requestInfo))
{}

void TCheckRangeActor::Bootstrap(const TActorContext& ctx)
{
    SendReadBlocksRequest(ctx);
    Become(&TThis::StateWork);
}

void TCheckRangeActor::SendReadBlocksRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();

    request->Record.SetStartIndex(Request.GetStartIndex());
    request->Record.SetBlocksCount(Request.GetBlocksCount());

    auto* headers = request->Record.MutableHeaders();

    headers->SetIsBackgroundRequest(true);
    NCloud::Send(ctx, Partition, std::move(request));
}

void TCheckRangeActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvVolume::TEvCheckRangeResponse>(std::move(error));

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

void TCheckRangeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvVolume::TEvCheckRangeResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCheckRangeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvService::TEvReadBlocksResponse, HandleReadBlocksResponse);
        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TCheckRangeActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "tablet is shutting down");

    ReplyAndDie(ctx, error);
}

void TCheckRangeActor::HandleReadBlocksResponse(
    const TEvService::TEvReadBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto response =
        std::make_unique<TEvVolume::TEvCheckRangeResponse>(MakeError(S_OK));

    const auto& error = msg->Record.GetError();
    if (HasError(error)) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::PARTITION,
            "reading error has occurred: " << FormatError(error));
        response->Record.MutableStatus()->CopyFrom(error);
    } else {
        if (Request.GetCalculateChecksums()) {
            TBlockChecksum blockChecksum;
            for (const auto& buffer: msg->Record.GetBlocks().GetBuffers()) {
                const auto checksum =
                    blockChecksum.Extend(buffer.data(), buffer.size());
                response->Record.MutableChecksums()->Add(checksum);
            }
        }
    }

    ReplyAndDie(ctx, std::move(response));
}

NProto::TError ValidateBlocksCount(
    ui64 blocksCount,
    ui64 bytesPerStripe,
    ui64 blockSize,
    ui64 checkRangeMaxRangeSize)
{
    ui64 maxBlocksPerRequest = Min<ui64>(
        bytesPerStripe / blockSize,
        checkRangeMaxRangeSize / blockSize);

    if (blocksCount > maxBlocksPerRequest) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Too many blocks requested: " << blocksCount
                << " Max blocks per request: " << maxBlocksPerRequest);
    }
    return {};
}

}   // namespace NCloud::NBlockStore::NStorage
