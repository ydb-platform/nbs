#include "part_nonrepl_actor.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/partition_common/actor_checkrange.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/xrange.h>
#include <util/stream/str.h>
#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TNonreplCheckRangeActor final: public TCheckRangeActor
{
public:
    using TCheckRangeActor::TCheckRangeActor;

    void Bootstrap(const TActorContext& ctx);

protected:
    void SendReadBlocksRequest(const TActorContext& ctx) override;

    void HandleReadBlocksResponse(
        const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
        const NActors::TActorContext& ctx) override;

};

////////////////////////////////////////////////////////////////////////////////

void TNonreplCheckRangeActor::SendReadBlocksRequest(const TActorContext& ctx)
{
    const TString clientId{CheckRangeClientId};

    TBlockRange64 range = TBlockRange64::WithLength(
        Request.GetStartIndex(),
        Request.GetBlocksCount());

    Buffer = TGuardedBuffer(TString::Uninitialized(range.Size() * BlockSize));

    auto sgList = Buffer.GetGuardedSgList();
    auto sgListOrError = SgListNormalize(sgList.Acquire().Get(), BlockSize);
    if (HasError(sgListOrError)){
        ReplyAndDie(ctx, sgListOrError.GetError());
        return;
    }
    SgList.SetSgList(sgListOrError.ExtractResult());

    auto request = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();

    request->Record.SetStartIndex(Request.GetStartIndex());
    request->Record.SetBlocksCount(Request.GetBlocksCount());
    request->Record.Sglist = SgList;

    auto* headers = request->Record.MutableHeaders();
    headers->SetClientId(clientId);
    headers->SetIsBackgroundRequest(true);
    request->Record.ShouldReportFailedRangesOnFailure = true;

    NCloud::Send(ctx, Partition, std::move(request));
}

void TNonreplCheckRangeActor::HandleReadBlocksResponse(
    const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
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

        auto* status = response->Record.MutableStatus();
        status->CopyFrom(error);

        if (!msg->Record.FailInfo.FailedRanges.empty()) {
            TStringBuilder builder;
            builder << "\n Fail in ranges: ["
                    << JoinRange(
                           ", ",
                           msg->Record.FailInfo.FailedRanges.begin(),
                           msg->Record.FailInfo.FailedRanges.end())
                    << "]";

            status->MutableMessage()->append(builder);
        }
    } else {
        if (Request.GetCalculateChecksums()) {
            TBlockChecksum blockChecksum;
            for (ui64 offset = 0, i = 0; i < Request.GetBlocksCount();
                 offset += BlockSize, ++i)
            {
                auto* data = Buffer.Get().data() + offset;
                const auto checksum = blockChecksum.Extend(data, BlockSize);
                response->Record.MutableChecksums()->Add(checksum);
            }
        }
    }

    ReplyAndDie(ctx, std::move(response));
}

}   // namespace

//////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionActor::HandleCheckRange(
    const TEvVolume::TEvCheckRangeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto& record = ev->Get()->Record;

    auto error = ValidateBlocksCount(
        record.GetBlocksCount(),
        Config->GetBytesPerStripe(),
        PartConfig->GetBlockSize(),
        Config->GetCheckRangeMaxRangeSize());

    if (HasError(error)) {
        auto response =
            std::make_unique<TEvVolume::TEvCheckRangeResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto actorId = NCloud::Register<TNonreplCheckRangeActor>(
        ctx,
        SelfId(),
        std::move(record),
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext),
        PartConfig->GetBlockSize());

    RequestsInProgress.AddReadRequest(actorId);
}

}   // namespace NCloud::NBlockStore::NStorage
