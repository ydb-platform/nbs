#include "part_actor.h"

#include "part_counters.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::UpdateStats(const NProto::TPartitionStats& update)
{
    State->UpdateStats([&] (NProto::TPartitionStats& stats) {
        UpdatePartitionCounters(stats, update);
    });

    auto blockSize = State->GetBlockSize();
    PartCounters->Cumulative.BytesWritten.Increment(
        update.GetUserWriteCounters().GetBlocksCount() * blockSize);

    PartCounters->Cumulative.BytesRead.Increment(
        update.GetUserReadCounters().GetBlocksCount() * blockSize);

    PartCounters->Cumulative.SysBytesWritten.Increment(
        update.GetSysWriteCounters().GetBlocksCount() * blockSize);

    PartCounters->Cumulative.SysBytesRead.Increment(
        update.GetSysReadCounters().GetBlocksCount() * blockSize);

    PartCounters->Cumulative.RealSysBytesWritten.Increment(
        update.GetRealSysWriteCounters().GetBlocksCount() * blockSize);

    PartCounters->Cumulative.RealSysBytesRead.Increment(
        update.GetRealSysReadCounters().GetBlocksCount() * blockSize);

    PartCounters->Cumulative.BatchCount.Increment(
        update.GetUserWriteCounters().GetBatchCount());
}

void TPartitionActor::UpdateActorStats(const TActorContext& ctx)
{
    if (PartCounters) {
        auto& actorQueue = PartCounters->Histogram.ActorQueue;
        auto& mailboxQueue = PartCounters->Histogram.MailboxQueue;

        auto actorQueues = ctx.CountMailboxEvents(1001);
        actorQueue.Increment(actorQueues.first);
        mailboxQueue.Increment(actorQueues.second);
    }
}

TPartitionStatisticsCounters TPartitionActor::GetStats(const TActorContext& ctx)
{
    PartCounters->Simple.MixedBytesCount.Set(
        State->GetMixedBlocksCount() * State->GetBlockSize());

    PartCounters->Simple.MergedBytesCount.Set(
        State->GetMergedBlocksCount() * State->GetBlockSize());

    PartCounters->Simple.FreshBytesCount.Set(
        State->GetUnflushedFreshBlocksCount() * State->GetBlockSize());

    PartCounters->Simple.UntrimmedFreshBlobBytesCount.Set(
        State->GetUntrimmedFreshBlobByteCount());

    PartCounters->Simple.UsedBytesCount.Set(
        State->GetUsedBlocksCount() * State->GetBlockSize());

    PartCounters->Simple.LogicalUsedBytesCount.Set(
        State->GetLogicalUsedBlocksCount() * State->GetBlockSize());

    // TODO: output new compaction score via another counter
    PartCounters->Simple.CompactionScore.Set(State->GetLegacyCompactionScore());

    PartCounters->Simple.CompactionGarbageScore.Set(
        State->GetCompactionGarbageScore());

    PartCounters->Simple.CompactionRangeCountPerRun.Set(
        State->GetCompactionRangeCountPerRun());

    PartCounters->Simple.BytesCount.Set(
        State->GetBlocksCount() * State->GetBlockSize());

    PartCounters->Simple.IORequestsInFlight.Set(State->GetIORequestsInFlight());

    PartCounters->Simple.IORequestsQueued.Set(State->GetIORequestsQueued());

    PartCounters->Simple.UsedBlocksMapMemSize.Set(
        State->GetUsedBlocks().MemSize());

    PartCounters->Simple.MixedIndexCacheMemSize.Set(
        State->GetMixedIndexCacheMemSize());

    PartCounters->Simple.AlmostFullChannelCount.Set(
        State->GetAlmostFullChannelCount());

    PartCounters->Simple.CleanupQueueBytes.Set(
        State->GetCleanupQueue().GetQueueBytes());
    PartCounters->Simple.GarbageQueueBytes.Set(
        State->GetGarbageQueue().GetGarbageQueueBytes());

    PartCounters->Simple.ChannelHistorySize.Set(ChannelHistorySize);

    PartCounters->Simple.CheckpointBytes.Set(State->CalculateCheckpointBytes());

    PartCounters->Simple.UnconfirmedBlobCount.Set(
        State->GetUnconfirmedBlobCount());
    PartCounters->Simple.ConfirmedBlobCount.Set(State->GetConfirmedBlobCount());

    ui64 sysCpuConsumption = 0;
    for (ui32 tx = 0; tx < TPartitionCounters::ETransactionType::TX_SIZE; ++tx)
    {
        sysCpuConsumption +=
            Counters->TxCumulative(tx, NKikimr::COUNTER_TT_EXECUTE_CPUTIME)
                .Get();
        sysCpuConsumption +=
            Counters->TxCumulative(tx, NKikimr::COUNTER_TT_BOOKKEEPING_CPUTIME)
                .Get();
    }

    NBlobMetrics::TBlobLoadMetrics blobLoadMetrics =
        NBlobMetrics::MakeBlobLoadMetrics(
            State->GetConfig().GetExplicitChannelProfiles(),
            *Executor()->GetResourceMetrics());
    NBlobMetrics::TBlobLoadMetrics offsetLoadMetrics =
        NBlobMetrics::TakeDelta(PrevMetrics, blobLoadMetrics);
    offsetLoadMetrics += OverlayMetrics;

    NKikimrTabletBase::TMetrics metrics;
    GetResourceMetrics()->FillChanged(
        metrics,
        ctx.Now(),
        true   // forceAll
    );

    TPartitionStatisticsCounters counters(
        sysCpuConsumption - SysCPUConsumption,
        UserCPUConsumption,
        std::move(PartCounters),
        std::move(offsetLoadMetrics),
        std::move(metrics));

    PrevMetrics = std::move(blobLoadMetrics);
    OverlayMetrics = {};

    UserCPUConsumption = 0;
    SysCPUConsumption = sysCpuConsumption;

    PartCounters = CreatePartitionDiskCounters(
        EPublishingPolicy::Repl,
        DiagnosticsConfig->GetHistogramCounterOptions());

    return counters;
}

void TPartitionActor::SendStatsToService(const TActorContext& ctx)
{
    if (!PartCounters) {
        return;
    }

    auto&& [diffSysCpuConsumption, userCpuConsumption, partCounters, offsetLoadMetrics, metrics] =
        GetStats(ctx);

    auto request = std::make_unique<TEvStatsService::TEvVolumePartCounters>(
        MakeIntrusive<TCallContext>(),
        State->GetConfig().GetDiskId(),
        std::move(partCounters),
        diffSysCpuConsumption,
        userCpuConsumption,
        !State->GetCheckpoints().IsEmpty(),
        std::move(offsetLoadMetrics),
        std::move(metrics));

    NCloud::Send(ctx, VolumeActorId, std::move(request));
}

void TPartitionActor::HandleGetPartCountersRequest(
    const TEvPartitionCommonPrivate::TEvGetPartCountersRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!PartCounters) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<
                TEvPartitionCommonPrivate::TEvGetPartCountersResponse>(
                MakeError(
                    EWellKnownResultCodes::E_INVALID_STATE,
                    "Empty PartCounters")));
        return;
    }

    auto&& [diffSysCpuConsumption, userCpuConsumption, partCounters, offsetLoadMetrics, metrics] =
        GetStats(ctx);

    auto response =
        std::make_unique<TEvPartitionCommonPrivate::TEvGetPartCountersResponse>(
            SelfId(),
            diffSysCpuConsumption,
            userCpuConsumption,
            std::move(partCounters),
            std::move(offsetLoadMetrics),
            std::move(metrics));

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
