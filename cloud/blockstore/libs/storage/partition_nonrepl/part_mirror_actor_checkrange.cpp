#include "part_mirror_actor.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/partition_common/actor_checkrange.h>

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
    bool ErrorOnReplicaReading{false};
    uint32_t ResponseCount{0};
    const TVector<TString> ReplicasNames;
    TVector<ui32> ReplicasSummaryChecksums;
    NCloud::NBlockStore::NProto::TCheckRangeResponse Response;

public:
    using TCheckRangeActor::TCheckRangeActor;
    template <typename... TArgs>
    explicit TMirrorCheckRangeActor(
        TVector<TString> replicasNames,
        TArgs&&... args)
        : TCheckRangeActor(std::forward<TArgs>(args)...)
        , ReplicasNames(std::move(replicasNames))
    {}

protected:
    bool OnMessage(TAutoPtr<NActors::IEventHandle>& ev) override;
    void SendReadBlocksRequest(const TActorContext& ctx) override;
    void HandleReadBlocksResponse(
        const TEvService::TEvReadBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
    void Done(const NActors::TActorContext& ctx);

private:
    void CalculateChecksums(const TEvService::TEvReadBlocksResponse::TPtr& ev);
    void HandleReadBlocksResponseError(
        const TEvService::TEvReadBlocksResponse::TPtr& ev,
        const TActorContext& ctx,
        const ::NCloud::NProto::TError& error);
};

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
    const TString clientId{CheckRangeClientId};

    for (ui32 i = 1; i < ReplicasNames.size() + 1; ++i) {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();

        request->Record.SetStartIndex(Request.GetStartIndex());
        request->Record.SetBlocksCount(Request.GetBlocksCount());

        auto* headers = request->Record.MutableHeaders();
        headers->SetReplicaIndex(i);
        headers->SetClientId(clientId);
        headers->SetIsBackgroundRequest(true);

        Response.MutableMirrorChecksums()->Add({});
        ReplicasSummaryChecksums.push_back(0);
        NCloud::Send(ctx, Partition, std::move(request), i);
    }
}

void TMirrorCheckRangeActor::HandleReadBlocksResponseError(
    const TEvService::TEvReadBlocksResponse::TPtr& ev,
    const TActorContext& ctx,
    const ::NCloud::NProto::TError& error)
{
    LOG_ERROR_S(
        ctx,
        TBlockStoreComponents::PARTITION,
        "reading error has occurred: " << FormatError(error));

    // 1 result error for all replicas
    ErrorOnReplicaReading = true;
    Response.MutableStatus()->SetCode(error.GetCode());
    *Response.MutableStatus()->MutableMessage() +=
        ReplicasNames[ev->Cookie - 1] + " ";
}

void TMirrorCheckRangeActor::CalculateChecksums(
    const TEvService::TEvReadBlocksResponse::TPtr& ev)
{
    ui32 replicaIdx = ev->Cookie - 1;
    auto& mirrorChecksums = (*Response.MutableMirrorChecksums())[replicaIdx];
    auto* replicaChecksums = mirrorChecksums.MutableChecksums();
    mirrorChecksums.SetReplicaName(std::move(ReplicasNames[replicaIdx]));

    TBlockChecksum summaryChecksum;
    for (const auto& buffer: ev->Get()->Record.GetBlocks().GetBuffers()) {
        summaryChecksum.Extend(buffer.data(), buffer.size());
        replicaChecksums->Add(
            TBlockChecksum().Extend(buffer.data(), buffer.size()));
    }

    ReplicasSummaryChecksums[replicaIdx] = summaryChecksum.GetValue();
}

void TMirrorCheckRangeActor::HandleReadBlocksResponse(
    const TEvService::TEvReadBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ++ResponseCount;
    const auto& error = ev->Get()->Record.GetError();
    if (HasError(error)) {
        HandleReadBlocksResponseError(ev, ctx, error);
    } else {
        CalculateChecksums(ev);
    }

    if (ResponseCount == ReplicasNames.size()) {
        Done(ctx);
    }
}

void TMirrorCheckRangeActor::Done(const NActors::TActorContext& ctx)
{
    auto response = std::make_unique<TEvVolume::TEvCheckRangeResponse>();
    bool checksumsEqual =
        std::ranges::adjacent_find(
            ReplicasSummaryChecksums,
            std::not_equal_to{}) == std::ranges::end(ReplicasSummaryChecksums);
    if (!ErrorOnReplicaReading && checksumsEqual) {
        *Response.MutableChecksums() =
            std::move((*Response.MutableMirrorChecksums())[0].GetChecksums());
        Response.ClearMirrorChecksums();
    } else {
        Response.ClearChecksums();
        if (!ErrorOnReplicaReading) {
            ui32 flags = 0;
            SetProtoFlag(flags, NProto::EF_CHECKSUM_MISMATCH);
            *Response.MutableStatus() = MakeError(
                E_IO,
                "Replicas checksum mismatch",
                flags);
        } // else using error that we have already
    }

    response->Record = std::move(Response);
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

    TVector<TString> replicaNames;
    for (const auto& replicaInfo: State.GetReplicaInfos()) {
        STORAGE_VERIFY(
            replicaInfo.Config->GetDevices().size() > 0,
            NCloud::TWellKnownEntityTypes::DISK,
            DiskId);
        replicaNames.push_back(
            replicaInfo.Config->GetDevices()[0].GetDeviceUUID());
    }
    NCloud::Register<TMirrorCheckRangeActor>(
        ctx,
        std::move(replicaNames),
        SelfId(),
        std::move(record),
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext),
        State.GetBlockSize());
}

}   // namespace NCloud::NBlockStore::NStorage
