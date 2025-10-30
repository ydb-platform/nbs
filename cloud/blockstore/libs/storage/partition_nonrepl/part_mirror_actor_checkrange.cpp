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
public:
    using TCheckRangeActor::TCheckRangeActor;
    template <typename... TArgs>
    TMirrorCheckRangeActor(TVector<TString>&& replicasNames, TArgs&&... args)
        : TCheckRangeActor(std::forward<TArgs>(args)...)
        , ReplicasNames(std::move(replicasNames))
    {}

    void Bootstrap(const TActorContext& ctx) override;
protected:
    STFUNC(StateWork);
    void SendReadBlocksRequest(const TActorContext& ctx) override;

    void HandleReadBlocksResponse(
        const TEvService::TEvReadBlocksLocalResponse::TPtr&,
        const NActors::TActorContext&) override
    {}
    void HandleReadBlocksResponseError(
        const TEvService::TEvReadBlocksLocalResponse::TPtr&,
        const TActorContext&,
        const ::NCloud::NProto::TError&,
        NProto::TError*) override
    {}
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
    static ui32 GetReplicaIdx(
        const TEvService::TEvReadBlocksResponse::TPtr& ev);

    uint32_t ResponseCount{0};
    TVector<TString> ReplicasNames;
    TVector<ui32> ReplicasSummaryChecksums;
    NCloud::NBlockStore::NProto::TCheckRangeResponse Response;
};

void TMirrorCheckRangeActor::Bootstrap(const TActorContext& ctx)
{
    SendReadBlocksRequest(ctx);
    Become(&TMirrorCheckRangeActor::StateWork);
}

STFUNC(TMirrorCheckRangeActor::StateWork)
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
        "reading error has been occurred: " << FormatError(error));

    // 1 result error for all replicas
    Response.MutableStatus()->SetCode(E_REJECTED);
    *Response.MutableStatus()->MutableMessage() +=
        ReplicasNames[GetReplicaIdx(ev)] + " ";
}

ui32 TMirrorCheckRangeActor::GetReplicaIdx(
    const TEvService::TEvReadBlocksResponse::TPtr& ev)
{
    return ev->Cookie - 1;
}

void TMirrorCheckRangeActor::CalculateChecksums(
    const TEvService::TEvReadBlocksResponse::TPtr& ev)
{
    ui32 replicaIdx = GetReplicaIdx(ev);
    auto& mirrorChecksums = (*Response.MutableMirrorChecksums())[replicaIdx];
    auto* replicaChecksums = mirrorChecksums.MutableChecksums();
    mirrorChecksums.SetReplicaName(std::move(ReplicasNames[replicaIdx]));

    TBlockChecksum summaryChecksum;
    ui32 summaryChecksumInt{};
    if (ev->Get()->Record.HasChecksum()) {
        summaryChecksumInt = ev->Get()->Record.GetChecksum().GetChecksum();
        for (const auto& buffer: ev->Get()->Record.GetBlocks().GetBuffers()) {
            replicaChecksums->Add(
                TBlockChecksum().Extend(buffer.data(), buffer.size()));
        }
    } else {
        for (const auto& buffer: ev->Get()->Record.GetBlocks().GetBuffers()) {
            summaryChecksum.Extend(buffer.data(), buffer.size());
            replicaChecksums->Add(
                TBlockChecksum().Extend(buffer.data(), buffer.size()));
        }
        summaryChecksumInt = summaryChecksum.GetValue();
    }

    ReplicasSummaryChecksums[replicaIdx] = summaryChecksumInt;
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
    if (checksumsEqual) {
        *Response.MutableChecksums() =
            std::move((*Response.MutableMirrorChecksums())[0].GetChecksums());
        Response.ClearMirrorChecksums();
    } else {
        Response.ClearChecksums();
        Response.MutableStatus()->SetCode(E_REJECTED);
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
