#include "part2_actor.h"

#include "part2_counters.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

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

void TPartitionActor::SendStatsToService(const TActorContext& ctx)
{
    if (!PartCounters) {
        return;
    }

    PartCounters->Simple.MixedBytesCount.Set(
        State->GetMixedBlockCount() * State->GetBlockSize());

    PartCounters->Simple.MergedBytesCount.Set(
        State->GetMergedBlockCount() * State->GetBlockSize());

    PartCounters->Simple.FreshBytesCount.Set(
        State->GetFreshBlockCount() * State->GetBlockSize());

    PartCounters->Simple.UsedBytesCount.Set(
        State->GetUsedBlockCount() * State->GetBlockSize());

    // TODO(NBS-2364): calculate logical used bytes count for partition2
    PartCounters->Simple.LogicalUsedBytesCount.Set(
        State->GetUsedBlockCount() * State->GetBlockSize());

    // TODO: output new compaction score via another counter
    PartCounters->Simple.CompactionScore.Set(
        State->GetLegacyCompactionScore());

    PartCounters->Simple.BytesCount.Set(
        State->GetBlockCount() * State->GetBlockSize());

    PartCounters->Simple.IORequestsInFlight.Set(
        State->GetIORequestsInFlight());

    PartCounters->Simple.IORequestsQueued.Set(
        State->GetIORequestsQueued());

    PartCounters->Simple.AlmostFullChannelCount.Set(
        State->GetAlmostFullChannelCount());

    PartCounters->Simple.CleanupQueueBytes.Set(0);
    PartCounters->Simple.GarbageQueueBytes.Set(
        State->GetGarbageQueue().GetGarbageQueueBytes());

    PartCounters->Simple.CheckpointBytes.Set(State->GetBlockSize()
        * State->GetCheckpoints().GetCheckpointBlockCount());

    PartCounters->Simple.ChannelHistorySize.Set(ChannelHistorySize);

    ui64 sysCpuConsumption  = 0;
    for (ui32 tx = 0; tx < TPartitionCounters::ETransactionType::TX_SIZE; ++tx) {
        sysCpuConsumption += Counters->TxCumulative(tx, NKikimr::COUNTER_TT_EXECUTE_CPUTIME).Get();
        sysCpuConsumption += Counters->TxCumulative(tx, NKikimr::COUNTER_TT_BOOKKEEPING_CPUTIME).Get();
    }

    NBlobMetrics::TBlobLoadMetrics blobLoadMetrics = NBlobMetrics::MakeBlobLoadMetrics(
        State->GetConfig().GetExplicitChannelProfiles(),
        *Executor()->GetResourceMetrics());
    NBlobMetrics::TBlobLoadMetrics offsetLoadMetrics = NBlobMetrics::TakeDelta(
        PrevMetrics, blobLoadMetrics);
    offsetLoadMetrics += OverlayMetrics;

    auto request = std::make_unique<TEvStatsService::TEvVolumePartCounters>(
        MakeIntrusive<TCallContext>(),
        State->GetConfig().GetDiskId(),
        std::move(PartCounters),
        sysCpuConsumption - SysCPUConsumption,
        UserCPUConsumption,
        !State->GetCheckpoints().IsEmpty(),
        std::move(offsetLoadMetrics));

    PrevMetrics = std::move(blobLoadMetrics);
    OverlayMetrics = {};

    UserCPUConsumption = 0;
    SysCPUConsumption = sysCpuConsumption;

    PartCounters = CreatePartitionDiskCounters(
        EPublishingPolicy::Repl,
        DiagnosticsConfig->GetHistogramCounterOptions());

    NCloud::Send(ctx, VolumeActorId, std::move(request));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
