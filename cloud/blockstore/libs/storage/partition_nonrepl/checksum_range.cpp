#include "checksum_range.h"

#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/disk_agent/public.h>

#include <contrib/ydb/library/actors/core/log.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TChecksumRangeActorCompanion::TChecksumRangeActorCompanion(
        TBlockRange64 range,
        TVector<TReplicaDescriptor> replicas)
    : Range(range)
    , Replicas(std::move(replicas))
{
    Checksums.resize(Replicas.size());
}

bool TChecksumRangeActorCompanion::IsFinished() {
    return Finished;
}

const TVector<ui64>& TChecksumRangeActorCompanion::GetChecksums() {
    return Checksums;
}

NProto::TError TChecksumRangeActorCompanion::GetError() {
    return Error;
}

void TChecksumRangeActorCompanion::ChecksumBlocks(const TActorContext& ctx) {
    for (size_t i = 0; i < Replicas.size(); ++i) {
        ChecksumReplicaBlocks(ctx, i);
    }
}

void TChecksumRangeActorCompanion::ChecksumReplicaBlocks(const TActorContext& ctx, int idx)
{
    auto request = std::make_unique<TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest>();
    request->Record.SetStartIndex(Range.Start);
    request->Record.SetBlocksCount(Range.Size());

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(TString(BackgroundOpsClientId));

    auto event = std::make_unique<NActors::IEventHandle>(
        Replicas[idx].ActorId,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        idx,          // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(event.release());
}

////////////////////////////////////////////////////////////////////////////////

void TChecksumRangeActorCompanion::HandleChecksumResponse(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    Error = msg->Record.GetError();

    if (HasError(Error)) {
        LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
            "[%s] Checksum error %s",
            Replicas[0].Name.c_str(),
            FormatError(Error).c_str());

        Finished = true;
        return;
    }

    Checksums[ev->Cookie] = msg->Record.GetChecksum();
    if (++ChecksumsCount == Replicas.size()) {
        Finished = true;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
