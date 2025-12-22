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
    const ui32 ReplicasNumber;
    ui32 ResponseCount = 0;
    TVector<ui32> ReplicasSummaryChecksums;
    TVector<NProto::TDiskChecksums> ReplicasChecksums;
    NProto::TError Status;

public:
    template <typename... TArgs>
    explicit TMirrorCheckRangeActor(
        ui32 replicasNumber,
        TArgs&&... args);

protected:
    bool OnMessage(TAutoPtr<NActors::IEventHandle>& ev) override;
    void SendReadBlocksRequest(const TActorContext& ctx) override;
    void HandleReadBlocksResponse(
        const TEvService::TEvReadBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
    void Done(const NActors::TActorContext& ctx);

private:
    void CalculateChecksums(ui32 replicaIndex, const NProto::TIOVector& iov);
};

template <typename... TArgs>
TMirrorCheckRangeActor::TMirrorCheckRangeActor(
        ui32 replicasNumber,
        TArgs&&... args)
    : TCheckRangeActor(std::forward<TArgs>(args)...)
    , ReplicasNumber{replicasNumber}
{}

bool TMirrorCheckRangeActor::OnMessage(TAutoPtr<NActors::IEventHandle>& ev)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvReadBlocksResponse, HandleReadBlocksResponse);
        default:
            return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorCheckRangeActor::SendReadBlocksRequest(const TActorContext& ctx)
{
    ReplicasSummaryChecksums.resize(ReplicasNumber);
    ReplicasChecksums.resize(ReplicasNumber);

    for (ui32 i = 1; i <= ReplicasNumber; ++i) {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();

        request->Record.SetStartIndex(Request.GetStartIndex());
        request->Record.SetBlocksCount(Request.GetBlocksCount());

        auto* headers = request->Record.MutableHeaders();
        headers->SetReplicaIndex(i);
        headers->SetClientId(TString(CheckRangeClientId));
        headers->SetIsBackgroundRequest(true);

        NCloud::Send(ctx, Partition, std::move(request), i);
    }
}

void TMirrorCheckRangeActor::CalculateChecksums(
    ui32 replicaIndex,
    const NProto::TIOVector& iov)
{
    auto* replicaChecksums = ReplicasChecksums[replicaIndex].MutableData();

    TBlockChecksum summaryChecksum;
    for (const auto& buffer: iov.GetBuffers()) {
        summaryChecksum.Extend(buffer.data(), buffer.size());
        replicaChecksums->Add(
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
    if (ResponseCount == ReplicasNumber) {
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
        response->Record.MutableDiskChecksums()->Swap(&ReplicasChecksums[0]);
    } else {
        response->Record.MutableMirrorChecksums()->MutableReplicas()->Assign(
            std::make_move_iterator(ReplicasChecksums.begin()),
            std::make_move_iterator(ReplicasChecksums.end()));
    }

    if (!HasError(status) && mismatch) {
        ui32 flags = 0;
        SetProtoFlag(flags, NProto::EF_CHECKSUM_MISMATCH);
        status = MakeError(E_IO, "Replicas checksum mismatch", flags);
    }

    ReplyAndDie(ctx, std::move(response));
}

}   // namespace

//////////////////////////////////////////////////////////////////

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
        auto response = std::make_unique<TEvVolume::TEvCheckRangeResponse>(
            std::move(error));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    NCloud::Register<TMirrorCheckRangeActor>(
        ctx,
        State.GetReplicaInfos().size(),
        SelfId(),
        std::move(record),
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext),
        State.GetBlockSize(),
        LogTitle.GetChild(GetCycleCount()));
}

}   // namespace NCloud::NBlockStore::NStorage
