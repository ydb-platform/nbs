#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>
#include <cloud/storage/core/libs/common/media.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCheckRangeActor final: public TActorBootstrapped<TCheckRangeActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const TString DiskId;
    const ui64 StartIndex;
    const ui64 BlocksCount;
    const bool CalculateChecksums;

public:
    TCheckRangeActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString diskId,
        ui64 startIndex,
        ui64 blocksCount,
        bool calculateChecksums);

    void Bootstrap(const TActorContext& ctx);

private:
    void CheckRange(const TActorContext& ctx);

    void HandleCheckRangeResponse(
        const TEvService::TEvCheckRangeResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvCheckRangeResponse> response);

private:
    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

TCheckRangeActor::TCheckRangeActor(
    TRequestInfoPtr requestInfo,
    TStorageConfigPtr config,
    TString diskId,
    ui64 startIndex,
    ui64 blocksCount,
    bool calculateChecksums)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , DiskId(std::move(diskId))
    , StartIndex(startIndex)
    , BlocksCount(blocksCount)
    , CalculateChecksums(calculateChecksums)
{}

void TCheckRangeActor::Bootstrap(const TActorContext& ctx)
{
    CheckRange(ctx);
}

void TCheckRangeActor::CheckRange(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    auto request = std::make_unique<TEvService::TEvCheckRangeRequest>();
    request->Record.SetDiskId(DiskId);
    request->Record.SetStartIndex(StartIndex);
    request->Record.SetBlocksCount(BlocksCount);
    request->Record.SetCalculateChecksums(CalculateChecksums);

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TCheckRangeActor::HandleCheckRangeResponse(
    const TEvService::TEvCheckRangeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& error = ev->Get()->GetError();
    auto response = std::make_unique<TEvService::TEvCheckRangeResponse>(error);
    response->Record.MutableStatus()->CopyFrom(ev->Get()->Record.GetStatus());
    response->Record.MutableChecksums()->Swap(ev->Get()->Record.MutableChecksums());

    ReplyAndDie(
        ctx,
        std::move(response));
}

void TCheckRangeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvCheckRangeResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCheckRangeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvCheckRangeResponse, HandleCheckRangeResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleCheckRange(
    const TEvService::TEvCheckRangeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    const auto& request = msg->Record;

    if (request.GetDiskId().empty()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Empty DiskId in CheckRange");

        auto response = std::make_unique<TEvService::TEvCheckRangeResponse>(
            MakeError(E_ARGUMENT, "Volume id cannot be empty"));

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (request.GetBlocksCount() == 0) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Zero BlockCounts in CheckRange");

        auto response = std::make_unique<TEvService::TEvCheckRangeResponse>(
            MakeError(E_ARGUMENT, "BlocksCounts shoud be more than zero"));

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "CheckRange volume: %s ",
        request.GetDiskId().Quote().data());

    NCloud::Register<TCheckRangeActor>(
        ctx,
        std::move(requestInfo),
        Config,
        request.GetDiskId(),
        request.GetStartIndex(),
        request.GetBlocksCount(),
        request.GetCalculateChecksums());
}

}   // namespace NCloud::NBlockStore::NStorage
