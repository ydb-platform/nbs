#include "part_mirror_actor.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>
#include <cloud/blockstore/libs/storage/partition_common/actor_checkrange.h>

#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/verify.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/xrange.h>
#include <util/stream/str.h>
#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMirrorCheckRangeActor final: public TCheckRangeActor
{
private:
    const ui32 TotalReplicaCount;
    ui32 ResponseCount = 0;
    TVector<ui32> ReplicasSummaryChecksums;
    TVector<NProto::TChecksums> ReplicasBlockByBlockChecksums;
    NProto::TError Status;

public:
    TMirrorCheckRangeActor(
        ui32 totalReplicaCount,
        const NActors::TActorId& partition,
        NProto::TCheckRangeRequest request,
        TRequestInfoPtr requestInfo,
        ui64 blockSize,
        TChildLogTitle logTitle);

protected:
    bool OnMessage(TAutoPtr<NActors::IEventHandle>& ev) override;
    void SendReadBlocksRequest(const TActorContext& ctx) override;
    void HandleReadBlocksResponse(
        const TEvService::TEvReadBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleReadUndelivery(
        const TEvService::TEvReadBlocksRequest::TPtr& ev,
        const TActorContext& ctx);
    void Done(const NActors::TActorContext& ctx);

private:
    void CalculateChecksums(ui32 replicaIndex, const NProto::TIOVector& iov);
};

TMirrorCheckRangeActor::TMirrorCheckRangeActor(
        ui32 totalReplicaCount,
        const NActors::TActorId& partition,
        NProto::TCheckRangeRequest request,
        TRequestInfoPtr requestInfo,
        ui64 blockSize,
        TChildLogTitle logTitle)
    : TCheckRangeActor(
          partition,
          std::move(request),
          std::move(requestInfo),
          blockSize,
          std::move(logTitle))
    , TotalReplicaCount{totalReplicaCount}
{}

bool TMirrorCheckRangeActor::OnMessage(TAutoPtr<NActors::IEventHandle>& ev)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvReadBlocksResponse, HandleReadBlocksResponse);
        HFunc(TEvService::TEvReadBlocksRequest, HandleReadUndelivery);
        default:
            return false;
    }

    return true;
}

void TMirrorCheckRangeActor::HandleReadUndelivery(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const ui32 replicaIndex = ev->Cookie - 1;
    TString errorMessage = TStringBuilder()
                           << "reading request for replica " << replicaIndex
                           << " is undelivered; ";
    LOG_ERROR_S(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        TStringBuilder() << LogTitle.GetWithTime() << ": " << errorMessage);

    Status = MakeError(E_REJECTED, errorMessage);
    Status.MutableMessage()->append(errorMessage);

    ++ResponseCount;
    if (ResponseCount == TotalReplicaCount) {
        Done(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorCheckRangeActor::SendReadBlocksRequest(const TActorContext& ctx)
{
    ReplicasSummaryChecksums.resize(TotalReplicaCount);
    ReplicasBlockByBlockChecksums.resize(TotalReplicaCount);

    for (ui32 i = 1; i <= TotalReplicaCount; ++i) {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();

        request->Record.SetStartIndex(Request.GetStartIndex());
        request->Record.SetBlocksCount(Request.GetBlocksCount());

        auto* headers = request->Record.MutableHeaders();
        headers->SetReplicaIndex(i);
        headers->SetClientId(TString(CheckRangeClientId));
        headers->SetIsBackgroundRequest(true);

        NCloud::SendWithUndeliveryTracking(ctx, Partition, std::move(request), i);
    }
}

void TMirrorCheckRangeActor::CalculateChecksums(
    ui32 replicaIndex,
    const NProto::TIOVector& iov)
{
    auto* replicasBlockByBlockChecksums =
        ReplicasBlockByBlockChecksums[replicaIndex].MutableData();

    TBlockChecksum summaryChecksum;
    for (const auto& buffer: iov.GetBuffers()) {
        summaryChecksum.Extend(buffer.data(), buffer.size());
        replicasBlockByBlockChecksums->Add(
            TBlockChecksum().Extend(buffer.data(), buffer.size()));
    }

    ReplicasSummaryChecksums[replicaIndex] = summaryChecksum.GetValue();
}

void TMirrorCheckRangeActor::HandleReadBlocksResponse(
    const TEvService::TEvReadBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const ui32 replicaIndex = ev->Cookie - 1;
    auto& record = ev->Get()->Record;

    if (HasError(record)) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::PARTITION_WORKER,
            LogTitle.GetWithTime() << " reading error has occurred: "
                                   << FormatError(record.GetError()));

        Status.Swap(record.MutableError());
        Status.MutableMessage()->append(
            TStringBuilder() << " replica index: " << replicaIndex << " ");
    } else {
        CalculateChecksums(replicaIndex, record.GetBlocks());
    }

    ++ResponseCount;
    if (ResponseCount == TotalReplicaCount) {
        Done(ctx);
    }
}

void TMirrorCheckRangeActor::Done(const NActors::TActorContext& ctx)
{
    auto response = std::make_unique<TEvVolume::TEvCheckRangeResponse>();
    auto& status = *response->Record.MutableStatus();
    status = std::move(Status);

    const bool mismatch =
        std::ranges::adjacent_find(
            ReplicasSummaryChecksums,
            std::not_equal_to{}) != ReplicasSummaryChecksums.end();

    if (!HasError(status) && !mismatch) {
        response->Record.MutableDiskChecksums()->Swap(
            &ReplicasBlockByBlockChecksums[0]);
        ReplyAndDie(ctx, std::move(response));
        return;
    }

    response->Record.MutableInconsistentChecksums()->MutableReplicas()->Assign(
        std::make_move_iterator(ReplicasBlockByBlockChecksums.begin()),
        std::make_move_iterator(ReplicasBlockByBlockChecksums.end()));

    if (!HasError(status) && mismatch) {
        ui32 flags = 0;
        SetProtoFlag(flags, NProto::EF_CHECKSUM_MISMATCH);
        status = MakeError(E_IO, "Replicas checksum mismatch", flags);
    }

    ReplyAndDie(ctx, std::move(response));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::ReplyError(
    const NActors::TActorContext& ctx,
    const TEvVolume::TEvCheckRangeRequest::TPtr& ev,
    NProto::TError&& error)
{
    LOG_ERROR_S(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        LogTitle.GetWithTime() << FormatError(error));
    auto response =
        std::make_unique<TEvVolume::TEvCheckRangeResponse>(std::move(error));
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TMirrorPartitionActor::HandleCheckRangeResponse(
    const TEvVolume::TEvCheckRangeResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto it = CheckRangeRequestsInfo.find(ev->Sender);
    if (it == CheckRangeRequestsInfo.end()) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::PARTITION_WORKER,
            LogTitle.GetWithTime()
                << "unexpected CheckRange response from actor: " << ev->Sender);
        return;
    }

    LockedRanges.RemoveRequest(it->second.Range);
    auto requestInfo = std::move(it->second.RequestInfo);
    CheckRangeRequestsInfo.erase(it);

    NCloud::Reply(
        ctx,
        *requestInfo,
        std::make_unique<TEvVolume::TEvCheckRangeResponse>(ev->Get()->Record));
}

void TMirrorPartitionActor::HandleCheckRange(
    const TEvVolume::TEvCheckRangeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto& record = ev->Get()->Record;

    auto error = ValidateBlocksCount(
        record.GetBlocksCount(),
        Config->GetBytesPerStripe(),
        State.GetBlockSize(),
        Config->GetCheckRangeMaxRangeSize());

    if (HasError(error)) {
        ReplyError(ctx, ev, std::move(error));
        return;
    }

    TBlockRange64 requestedRange = TBlockRange64::WithLength(
        record.GetStartIndex(),
        record.GetBlocksCount());

    if (RequestsInProgress.OverlapsWithWrites(requestedRange)) {
        ReplyError(
            ctx,
            ev,
            MakeError(
                E_REJECTED,
                TStringBuilder()
                    << requestedRange.Print()
                    << ": you have to wait until active write request ends"));
        return;
    }

    if (LockedRanges.OverlapsWithRequest(requestedRange)) {
        ReplyError(
            ctx,
            ev,
            MakeError(
                E_REJECTED,
                TStringBuilder()
                    << requestedRange.Print()
                    << ": you have to wait until this range unlocks"));
        return;
    }

    LockedRanges.AddRequest(requestedRange);
    auto worker = NCloud::Register<TMirrorCheckRangeActor>(
        ctx,
        State.GetReplicaInfos().size(),
        SelfId(),
        std::move(record),
        CreateRequestInfo(SelfId(), ev->Cookie, ev->Get()->CallContext),
        State.GetBlockSize(),
        LogTitle.GetChild(GetCycleCount()));
    CheckRangeRequestsInfo[worker] = TCheckRangeRequestInfo(
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext),
        requestedRange);
}

}   // namespace NCloud::NBlockStore::NStorage
