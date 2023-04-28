#include "part2_diagnostics.h"

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

template <>
void TPartitionActor::LogBlockInfos<EBlockStoreRequest>(
    const NActors::TActorContext& ctx,
    EBlockStoreRequest requestType,
    TVector<IProfileLog::TBlockInfo> blockInfos,
    ui64 commitId)
{
    if (blockInfos.empty()) {
        return;
    }

    IProfileLog::TReadWriteRequestBlockInfos request;
    request.RequestType = requestType;
    request.BlockInfos = std::move(blockInfos);
    request.CommitId = commitId;

    IProfileLog::TRecord record;
    record.DiskId = State->GetConfig().GetDiskId();
    record.Ts = ctx.Now();
    record.Request = std::move(request);

    ProfileLog->Write(std::move(record));
}

template <>
void TPartitionActor::LogBlockInfos<ESysRequestType>(
    const NActors::TActorContext& ctx,
    ESysRequestType requestType,
    TVector<IProfileLog::TBlockInfo> blockInfos,
    ui64 commitId)
{
    if (blockInfos.empty()) {
        return;
    }

    IProfileLog::TSysReadWriteRequestBlockInfos request;
    request.RequestType = requestType;
    request.BlockInfos = std::move(blockInfos);
    request.CommitId = commitId;

    IProfileLog::TRecord record;
    record.DiskId = State->GetConfig().GetDiskId();
    record.Ts = ctx.Now();
    record.Request = std::move(request);

    ProfileLog->Write(std::move(record));
}

void TPartitionActor::LogBlockCommitIds(
    const NActors::TActorContext& ctx,
    ESysRequestType requestType,
    TVector<IProfileLog::TBlockCommitId> blockCommitIds,
    ui64 commitId)
{
    if (blockCommitIds.empty()) {
        return;
    }

    IProfileLog::TSysReadWriteRequestBlockCommitIds request;
    request.RequestType = requestType;
    request.BlockCommitIds = std::move(blockCommitIds);
    request.CommitId = commitId;

    IProfileLog::TRecord record;
    record.DiskId = State->GetConfig().GetDiskId();
    record.Ts = ctx.Now();
    record.Request = std::move(request);

    ProfileLog->Write(std::move(record));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
