#include "actor_checkrange.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <util/generic/string.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TCheckRangeActor::TCheckRangeActor(
    const TActorId& partition,
    NProto::TCheckRangeRequest request,
    TRequestInfoPtr requestInfo,
    ui64 blockSize,
    TChildLogTitle logTitle)
    : Partition(partition)
    , Request(std::move(request))
    , RequestInfo(std::move(requestInfo))
    , BlockSize(blockSize)
    , LogTitle(std::move(logTitle))
{}

void TCheckRangeActor::Bootstrap(const TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "%s CheckRangeActor has started",
        LogTitle.GetWithTime().c_str());

    Become(&TThis::StateWork);
    SendReadBlocksRequest(ctx);
}

void TCheckRangeActor::SendReadBlocksRequest(const TActorContext& ctx)
{
    TBlockRange64 range = TBlockRange64::WithLength(
        Request.GetStartIndex(),
        Request.GetBlocksCount());

    Buffer = TGuardedBuffer(TString::Uninitialized(range.Size() * BlockSize));

    auto sgList = Buffer.GetGuardedSgList();
    auto sgListOrError = SgListNormalize(sgList.Acquire().Get(), BlockSize);
    if (HasError(sgListOrError)) {
        ReplyAndDie(ctx, sgListOrError.GetError());
        return;
    }
    SgList.SetSgList(sgListOrError.ExtractResult());

    auto request = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();

    request->Record.SetStartIndex(Request.GetStartIndex());
    request->Record.SetBlocksCount(Request.GetBlocksCount());
    request->Record.Sglist = SgList;
    request->Record.ShouldReportFailedRangesOnFailure = true;

    auto* headers = request->Record.MutableHeaders();
    headers->SetClientId(TString(CheckRangeClientId));
    headers->SetIsBackgroundRequest(true);

    NCloud::Send(ctx, Partition, std::move(request));
}

void TCheckRangeActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "%s CheckRangeActor has finished",
        LogTitle.GetWithTime().c_str());

    auto response = std::make_unique<TEvVolume::TEvCheckRangeResponse>(error);
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

void TCheckRangeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvVolume::TEvCheckRangeResponse> response)
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "%s CheckRangeActor has finished",
        LogTitle.GetWithTime().c_str());

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////
bool TCheckRangeActor::OnMessage(TAutoPtr<NActors::IEventHandle>&)
{
    return false;
}

STFUNC(TCheckRangeActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    if (OnMessage(ev)) {
        return;
    }

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvReadBlocksLocalResponse, HandleReadBlocksResponse);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
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

NProto::TError TCheckRangeActor::HandleReadBlocksResponseError(
    const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
    const NActors::TActorContext& ctx,
    const ::NCloud::NProto::TError& error)
{
    const auto* msg = ev->Get();
    LOG_ERROR_S(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        LogTitle.GetWithTime()
            << " reading error has occurred: " << FormatError(error));

    NProto::TError result(error);
    if (!msg->Record.FailInfo.FailedRanges.empty()) {
        TStringBuilder builder;
        builder << ", Fail in ranges:\n ["
                << JoinRange(
                       ", ",
                       msg->Record.FailInfo.FailedRanges.begin(),
                       msg->Record.FailInfo.FailedRanges.end())
                << "]";

        result.MutableMessage()->append(builder);
    }

    return result;
}

void TCheckRangeActor::HandleReadBlocksResponse(
    const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto response = std::make_unique<TEvVolume::TEvCheckRangeResponse>();

    const auto& error = msg->Record.GetError();
    if (HasError(error)) {
        auto err = HandleReadBlocksResponseError(ev, ctx, error);
        response->Record.MutableStatus()->CopyFrom(err);
    } else {
        for (ui64 offset = 0, i = 0; i < Request.GetBlocksCount();
             offset += BlockSize, ++i)
        {
            const char* data = Buffer.Get().data() + offset;
            response->Record.MutableChecksums()->Add(
                TBlockChecksum().Extend(data, BlockSize));
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
