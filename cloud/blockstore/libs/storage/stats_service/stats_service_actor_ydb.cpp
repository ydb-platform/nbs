#include "stats_service_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/ydbstats/ydbstats.h>
#include <cloud/storage/core/libs/diagnostics/histogram.h>
#include <cloud/storage/core/libs/diagnostics/weighted_percentile.h>

#include <contrib/ydb/core/base/appdata.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NMonitoring;
using namespace NProto;
using namespace NYdbStats;

namespace {

////////////////////////////////////////////////////////////////////////////////

enum EPercentileIndex
{
    PERCENTILE_50,
    PERCENTILE_90,
    PERCENTILE_99,
    PERCENTILE_999,
    PERCENTILE_100
};

NYdbStats::TCumulativeCounterField CreateCumulativeCounterField(
    const TCumulativeCounter& counter,
    TDuration updateInterval,
    TDuration uploadInterval)
{
    auto min = counter.MinValue / updateInterval.Seconds();
    auto max = counter.MaxValue / updateInterval.Seconds();
    return {max , (min <= max) ? min : 0, counter.Sum / uploadInterval.Seconds()};
}

NYdbStats::TPercentileCounterField CreatePercentileCounterField(const TVector<double>& source)
{
    NYdbStats::TPercentileCounterField out;
    out.P50 = source[PERCENTILE_50];
    out.P90 = source[PERCENTILE_90];
    out.P99 = source[PERCENTILE_99];
    out.P999 = source[PERCENTILE_999];
    out.P100 = source[PERCENTILE_100];
    return out;
}

NYdbStats::TYdbRow BuildStatsForUpload(
    const TActorContext& ctx,
    const TVolumeStatsInfo& volume,
    TDuration updateInterval,
    TDuration uploadInterval)
{
    TYdbRow out;

    out.DiskId = volume.VolumeInfo.GetDiskId();
    out.Timestamp = ctx.Now().Seconds();
    out.FolderId = volume.VolumeInfo.GetFolderId();
    out.CloudId = volume.VolumeInfo.GetCloudId();
    out.BlocksCount = volume.VolumeInfo.GetBlocksCount();
    out.BlockSize = volume.VolumeInfo.GetBlockSize();
    out.StorageMediaKind =
        static_cast<ui64>(volume.VolumeInfo.GetStorageMediaKind());
    out.HostName = FQDNHostName();

    const auto& disk = volume.PerfCounters;

#define BLOCKSTORE_SIMPLE_COUNTER(counter)                                     \
        out.counter = disk.YdbDiskCounters.Simple.counter.Value;               \
//  BLOCKSTORE_SIMPLE_COUNTER

    BLOCKSTORE_SIMPLE_COUNTER(MixedBytesCount);
    BLOCKSTORE_SIMPLE_COUNTER(MergedBytesCount);
    BLOCKSTORE_SIMPLE_COUNTER(FreshBytesCount);
    BLOCKSTORE_SIMPLE_COUNTER(UsedBytesCount);
    BLOCKSTORE_SIMPLE_COUNTER(LogicalUsedBytesCount);
    BLOCKSTORE_SIMPLE_COUNTER(BytesCount);
    BLOCKSTORE_SIMPLE_COUNTER(IORequestsInFlight);
    BLOCKSTORE_SIMPLE_COUNTER(IORequestsQueued);
    BLOCKSTORE_SIMPLE_COUNTER(UsedBlocksMapMemSize);
    BLOCKSTORE_SIMPLE_COUNTER(CheckpointBytes);
    BLOCKSTORE_SIMPLE_COUNTER(AlmostFullChannelCount);
    BLOCKSTORE_SIMPLE_COUNTER(FreshBlocksInFlight);
    BLOCKSTORE_SIMPLE_COUNTER(FreshBlocksQueued);
    BLOCKSTORE_SIMPLE_COUNTER(CompactionScore);
    BLOCKSTORE_SIMPLE_COUNTER(CompactionGarbageScore);
    BLOCKSTORE_SIMPLE_COUNTER(CleanupQueueBytes);
    BLOCKSTORE_SIMPLE_COUNTER(GarbageQueueBytes);
#undef BLOCKSTORE_SIMPLE_COUNTER


#define BLOCKSTORE_SIMPLE_COUNTER(counter)                                     \
        out.counter = disk.VolumeSelfCounters.Simple.counter.Value;            \
//  BLOCKSTORE_SIMPLE_COUNTER

    BLOCKSTORE_SIMPLE_COUNTER(VolumeTabletId);
    BLOCKSTORE_SIMPLE_COUNTER(MaxReadBandwidth);
    BLOCKSTORE_SIMPLE_COUNTER(MaxWriteBandwidth);
    BLOCKSTORE_SIMPLE_COUNTER(MaxReadIops);
    BLOCKSTORE_SIMPLE_COUNTER(MaxWriteIops);
    BLOCKSTORE_SIMPLE_COUNTER(RealMaxWriteBandwidth);
    BLOCKSTORE_SIMPLE_COUNTER(PostponedQueueWeight);
    BLOCKSTORE_SIMPLE_COUNTER(BPFreshIndexScore);
    BLOCKSTORE_SIMPLE_COUNTER(BPCompactionScore);
    BLOCKSTORE_SIMPLE_COUNTER(BPDiskSpaceScore);
    BLOCKSTORE_SIMPLE_COUNTER(BPCleanupScore);
    BLOCKSTORE_SIMPLE_COUNTER(PartitionCount);

#undef BLOCKSTORE_SIMPLE_COUNTER

#define BLOCKSTORE_CUMULATIVE_COUNTER(counter)                                 \
    CreateCumulativeCounterField(                                              \
        disk.YdbDiskCounters.Cumulative.counter,                               \
        updateInterval,                                                        \
        uploadInterval);                                                       \
//  BLOCKSTORE_CUMULATIVE_COUNTER

    out.Write_Throughput = BLOCKSTORE_CUMULATIVE_COUNTER(BytesWritten);
    out.Read_Throughput = BLOCKSTORE_CUMULATIVE_COUNTER(BytesRead);
    out.SysWrite_Throughput = BLOCKSTORE_CUMULATIVE_COUNTER(SysBytesWritten);
    out.SysRead_Throughput = BLOCKSTORE_CUMULATIVE_COUNTER(SysBytesRead);
    out.RealSysWrite_Throughput = BLOCKSTORE_CUMULATIVE_COUNTER(RealSysBytesWritten);
    out.RealSysRead_Throughput = BLOCKSTORE_CUMULATIVE_COUNTER(RealSysBytesRead);
    out.UncompressedWrite_Throughput =
        BLOCKSTORE_CUMULATIVE_COUNTER(UncompressedBytesWritten);
    out.CompressedWrite_Throughput =
        BLOCKSTORE_CUMULATIVE_COUNTER(CompressedBytesWritten);
    out.CompactionByReadStats_Throughput =
        BLOCKSTORE_CUMULATIVE_COUNTER(CompactionByReadStats);
    out.CompactionByBlobCountPerRange_Throughput =
        BLOCKSTORE_CUMULATIVE_COUNTER(CompactionByBlobCountPerRange);
    out.CompactionByBlobCountPerDisk_Throughput =
        BLOCKSTORE_CUMULATIVE_COUNTER(CompactionByBlobCountPerDisk);
    out.CompactionByGarbageBlocksPerRange_Throughput =
        BLOCKSTORE_CUMULATIVE_COUNTER(CompactionByGarbageBlocksPerRange);
    out.CompactionByGarbageBlocksPerDisk_Throughput =
        BLOCKSTORE_CUMULATIVE_COUNTER(CompactionByGarbageBlocksPerDisk);

#undef BLOCKSTORE_CUMULATIVE_COUNTER

#define BLOCKSTORE_CUMULATIVE_COUNTER(counter)                                 \
    CreateCumulativeCounterField(                                              \
        disk.YdbVolumeSelfCounters.Cumulative.counter,                         \
        updateInterval,                                                        \
        uploadInterval);                                                       \
//  BLOCKSTORE_CUMULATIVE_COUNTER

    out.ThrottlerRejectedRequests =
    BLOCKSTORE_CUMULATIVE_COUNTER(ThrottlerRejectedRequests);
    out.ThrottlerPostponedRequests =
    BLOCKSTORE_CUMULATIVE_COUNTER(ThrottlerPostponedRequests);
    out.ThrottlerSkippedRequests =
    BLOCKSTORE_CUMULATIVE_COUNTER(ThrottlerSkippedRequests);

#undef BLOCKSTORE_CUMULATIVE_COUNTER

    auto& r = disk.YdbDiskCounters.RequestCounters;

#define BLOCKSTORE_PERCENTILE_COUNTER(counter)                                 \
    out.counter = CreatePercentileCounterField(                                \
            r.counter.GetTotal().CalculatePercentiles());                      \
//  BLOCKSTORE_PERCENTILE_COUNTER_WITH_SIZE

    BLOCKSTORE_PERCENTILE_COUNTER(Flush);
    BLOCKSTORE_PERCENTILE_COUNTER(AddBlobs);
    BLOCKSTORE_PERCENTILE_COUNTER(Compaction);
    BLOCKSTORE_PERCENTILE_COUNTER(Cleanup);
    BLOCKSTORE_PERCENTILE_COUNTER(CollectGarbage);
    BLOCKSTORE_PERCENTILE_COUNTER(DeleteGarbage);
#undef BLOCKSTORE_PERCENTILE_COUNTER

    auto& h = disk.YdbVolumeSelfCounters.RequestCounters;

#define BLOCKSTORE_PERCENTILE_COUNTER(counter)                                 \
    out.counter##ThrottlerDelay = CreatePercentileCounterField(                \
            h.counter.CalculatePercentiles());                                 \
//  BLOCKSTORE_PERCENTILE_COUNTER

    BLOCKSTORE_PERCENTILE_COUNTER(ReadBlocks);
    BLOCKSTORE_PERCENTILE_COUNTER(WriteBlocks);
    BLOCKSTORE_PERCENTILE_COUNTER(ZeroBlocks);

#undef BLOCKSTORE_PERCENTILE_COUNTER
    return out;
}

NYdbStats::TYdbBlobLoadMetricRow BuildBlobLoadMetricsForUpload(
    const TActorContext& ctx,
    const NBlobMetrics::TBlobLoadMetrics& metrics)
{
    NJsonWriter::TBuf result;

    result.BeginList();

    result.BeginList()
        .WriteString("Kind")
        .WriteString("GroupId")
        .WriteString("ReadByteCount")
        .WriteString("ReadByteIops")
        .WriteString("WriteByteCount")
        .WriteString("WriteByteIops");
    result.EndList();

    for (const auto& kind: metrics.PoolKind2TabletOps) {
        THashMap<NBlobMetrics::TBlobLoadMetrics::TGroupId,
                 NBlobMetrics::TBlobLoadMetrics::TTabletMetric> agregate;
        for (const auto& operation: kind.second) {
            agregate[operation.first.second] += operation.second;
        }
        for (const auto& operation: agregate) {
            result.BeginList()
                .WriteString(kind.first)
                .WriteULongLong(operation.first)
                .WriteULongLong(operation.second.ReadOperations.ByteCount)
                .WriteULongLong(operation.second.ReadOperations.Iops)
                .WriteULongLong(operation.second.WriteOperations.ByteCount)
                .WriteULongLong(operation.second.WriteOperations.Iops);
            result.EndList();
        }
    }

    result.EndList();

    return {FQDNHostName(), ctx.Now(), result.Str()};
}

}    // namespace

////////////////////////////////////////////////////////////////////////////////

struct TStatsServiceActor::TActorSystemHolder
{
    TActorSystem* const ActorSystem;

    TActorSystemHolder(TActorSystem* actorSystem)
        : ActorSystem(actorSystem)
    {}

    bool Send(const TActorId& recipient, IEventBasePtr event)
    {
        return ActorSystem->Send(recipient, event.release(), 0);
    }
};

////////////////////////////////////////////////////////////////////////////////

void TStatsServiceActor::Registered(TActorSystem* sys, const TActorId& owner)
{
    ActorSystemHolder = std::make_shared<TActorSystemHolder>(sys);
    TActorBootstrapped<TStatsServiceActor>::Registered(sys, owner);
}

void TStatsServiceActor::ScheduleStatsUpload(const TActorContext& ctx)
{
    if (StatsUploadScheduled) {
        return;
    }

    auto time = Config->GetStatsUploadInterval();
    if (time) {
        const double volumeCount = State.GetVolumes().size();
        const auto batchSize = Config->GetStatsUploadDiskCount();

        if (volumeCount > batchSize) {
            const auto factor = batchSize / volumeCount;
            time *= factor;
        }

        LOG_DEBUG(ctx, TBlockStoreComponents::STATS_SERVICE,
            "Stats uploading scheduled in %u seconds", time.Seconds());
        ctx.Schedule(time, new TEvStatsServicePrivate::TEvUploadDisksStats());
    }
}

void TStatsServiceActor::HandleStatsUploadRetryTimeout(
    const TEvStatsServicePrivate::TEvStatsUploadRetryTimeout::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    PushYdbStats(ctx);
}

void TStatsServiceActor::HandleUploadDisksStatsCompleted(
    const TEvStatsServicePrivate::TEvUploadDisksStatsCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto status = msg->GetStatus();

    State.SetStatsUploadingCompleted(true);

    if (FAILED(status)) {
        LOG_ERROR(ctx, TBlockStoreComponents::STATS_SERVICE,
            "Stats upload failed with code (%u) and message: %s",
            status,
            msg->GetErrorReason().data());

        if (YDbFailedRequests) {
            YDbFailedRequests->Add(1);
        }

        ctx.Schedule(
            Config->GetStatsUploadRetryTimeout(),
            new TEvStatsServicePrivate::TEvStatsUploadRetryTimeout());
    } else {
        YdbStatsRequests.pop_front();

        PushYdbStats(ctx);
    }
}

void TStatsServiceActor::PushYdbStats(const NActors::TActorContext& ctx)
{
    if (State.GetStatsUploadingCompleted()) {
        const auto minTs = ctx.Now() - Config->GetStatsUploadInterval();

        while (YdbStatsRequests && YdbStatsRequests.front().second <= minTs) {
            YdbStatsRequests.pop_front();
        }

        if (!YdbStatsRequests) {
            return;
        }

        auto replyTo = SelfId();
        auto weak = std::weak_ptr<TActorSystemHolder>(ActorSystemHolder);

        State.SetStatsUploadingCompleted(false);

        LOG_DEBUG_S(ctx, TBlockStoreComponents::STATS_SERVICE,
            "Send volumes stats for " <<
            YdbStatsRequests.front().first.Stats.size() <<
            " disks");

        YdbStatsRequestSentTs = ctx.Now();

        StatsUploader->UploadStats(
            YdbStatsRequests.front().first.Stats,
            YdbStatsRequests.front().first.Metrics).Subscribe(
            [=] (const auto& future) {
                if (auto p = weak.lock()) {
                    NProto::TError result;
                    try {
                        result = future.GetValue();
                    } catch (...) {
                        result = MakeError(E_FAIL, TStringBuilder()
                            << "Ydb request failed with error: "
                            << CurrentExceptionMessage());
                    }

                    using TMsg = TEvStatsServicePrivate::TEvUploadDisksStatsCompleted;
                    p->Send(replyTo, std::make_unique<TMsg>(result));
                }
        });
    } else if (YdbStatsRequestSentTs.GetValue()) {
        const auto deadline =
            YdbStatsRequestSentTs + Config->GetStatsUploadInterval();
        const auto now = ctx.Now();

        if (deadline < now) {
            LOG_WARN(ctx, TBlockStoreComponents::STATS_SERVICE,
                "YdbStatsRequest hanging, sent at %s, now %s",
                YdbStatsRequestSentTs.ToString().c_str(),
                now.ToString().c_str());

            ReportHangingYdbStatsRequest();
        }
    }
}

void TStatsServiceActor::HandleUploadDisksStats(
    const TEvStatsServicePrivate::TEvUploadDisksStats::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    StatsUploadScheduled = false;

    if (!VolumeIdQueueForYdbStatsUpload) {
        for (auto& p: State.GetVolumes()) {
            VolumeIdQueueForYdbStatsUpload.push_back(p.first);
        }
    }

    TStatsUploadRequest result;
    result.second = ctx.Now();
    ui32 volumeCnt = 0;

    while (VolumeIdQueueForYdbStatsUpload
            && volumeCnt < Config->GetStatsUploadDiskCount())
    {
        auto volumeInfoPtr =
            State.GetVolume(VolumeIdQueueForYdbStatsUpload.front());

        VolumeIdQueueForYdbStatsUpload.pop_front();

        if (!volumeInfoPtr) {
            continue;
        }

        ++volumeCnt;

        result.first.Stats.emplace_back(BuildStatsForUpload(
            ctx,
            *volumeInfoPtr,
            UpdateCountersInterval,
            Config->GetStatsUploadInterval()
        ));

        volumeInfoPtr->PerfCounters.YdbDiskCounters.Reset();
        volumeInfoPtr->PerfCounters.YdbVolumeSelfCounters.Reset();
    }

    if (((ctx.Now() - YdbMetricsRequestSentTs) > Config->GetStatsUploadInterval())
        && (!CurrentBlobMetrics.PoolKind2TabletOps.empty())) {

        YdbMetricsRequestSentTs = ctx.Now();

        result.first.Metrics.emplace_back(BuildBlobLoadMetricsForUpload(
            ctx,
            CurrentBlobMetrics
        ));
    }

    if (result.first.Stats.size() || result.first.Metrics.size()) {
        YdbStatsRequests.push_back(std::move(result));

        PushYdbStats(ctx);
    }

    ScheduleStatsUpload(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
