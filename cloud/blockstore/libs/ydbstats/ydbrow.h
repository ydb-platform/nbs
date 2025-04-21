#pragma once

#include <contrib/ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NYdbStats {

////////////////////////////////////////////////////////////////////////////////

#define YDB_SIMPLE_STRING_COUNTERS(xxx, ...)                                   \
    xxx(DiskId,                      __VA_ARGS__)                              \
    xxx(FolderId,                    __VA_ARGS__)                              \
    xxx(CloudId,                     __VA_ARGS__)                              \
    xxx(HostName,                    __VA_ARGS__)                              \
// YDB_SIMPLE_STRING_COUNTERS

#define YDB_SIMPLE_UINT64_COUNTERS(xxx, ...)                                   \
    xxx(VolumeTabletId,              __VA_ARGS__)                              \
    xxx(Timestamp,                   __VA_ARGS__)                              \
    xxx(BlocksCount,                 __VA_ARGS__)                              \
    xxx(BlockSize,                   __VA_ARGS__)                              \
    xxx(StorageMediaKind,            __VA_ARGS__)                              \
    xxx(MixedBytesCount,             __VA_ARGS__)                              \
    xxx(MergedBytesCount,            __VA_ARGS__)                              \
    xxx(FreshBytesCount,             __VA_ARGS__)                              \
    xxx(UsedBytesCount,              __VA_ARGS__)                              \
    xxx(LogicalUsedBytesCount,       __VA_ARGS__)                              \
    xxx(CompactionScore,             __VA_ARGS__)                              \
    xxx(BytesCount,                  __VA_ARGS__)                              \
    xxx(IORequestsInFlight,          __VA_ARGS__)                              \
    xxx(IORequestsQueued,            __VA_ARGS__)                              \
    xxx(UsedBlocksMapMemSize,        __VA_ARGS__)                              \
    xxx(MixedIndexCacheMemSize,      __VA_ARGS__)                              \
    xxx(MaxReadBandwidth,            __VA_ARGS__)                              \
    xxx(MaxWriteBandwidth,           __VA_ARGS__)                              \
    xxx(MaxReadIops,                 __VA_ARGS__)                              \
    xxx(MaxWriteIops,                __VA_ARGS__)                              \
    xxx(BurstPercentage,             __VA_ARGS__)                              \
    xxx(BoostPercentage,             __VA_ARGS__)                              \
    xxx(MaxPostponedWeight,          __VA_ARGS__)                              \
    xxx(BoostTime,                   __VA_ARGS__)                              \
    xxx(BoostRefillTime,             __VA_ARGS__)                              \
    xxx(ThrottlingEnabled,           __VA_ARGS__)                              \
    xxx(RealMaxWriteBandwidth,       __VA_ARGS__)                              \
    xxx(PostponedQueueWeight,        __VA_ARGS__)                              \
    xxx(BPFreshIndexScore,           __VA_ARGS__)                              \
    xxx(BPCompactionScore,           __VA_ARGS__)                              \
    xxx(BPDiskSpaceScore,            __VA_ARGS__)                              \
    xxx(BPCleanupScore,              __VA_ARGS__)                              \
    xxx(CheckpointBytes,             __VA_ARGS__)                              \
    xxx(AlmostFullChannelCount,      __VA_ARGS__)                              \
    xxx(FreshBlocksInFlight,         __VA_ARGS__)                              \
    xxx(FreshBlocksQueued,           __VA_ARGS__)                              \
    xxx(CompactionGarbageScore,      __VA_ARGS__)                              \
    xxx(CleanupQueueBytes,           __VA_ARGS__)                              \
    xxx(GarbageQueueBytes,           __VA_ARGS__)                              \
    xxx(PartitionCount,              __VA_ARGS__)                              \
    xxx(CompactionRangeCountPerRun,  __VA_ARGS__)                              \
// YDB_SIMPLE_UINT64_COUNTERS

#define YDB_CUMULATIVE_COUNTERS(xxx, ...)                                      \
    xxx(Write_Throughput,            __VA_ARGS__)                              \
    xxx(Read_Throughput,             __VA_ARGS__)                              \
    xxx(SysWrite_Throughput,         __VA_ARGS__)                              \
    xxx(SysRead_Throughput,          __VA_ARGS__)                              \
    xxx(RealSysWrite_Throughput,     __VA_ARGS__)                              \
    xxx(RealSysRead_Throughput,      __VA_ARGS__)                              \
    xxx(ThrottlerRejectedRequests,   __VA_ARGS__)                              \
    xxx(ThrottlerPostponedRequests,  __VA_ARGS__)                              \
    xxx(ThrottlerSkippedRequests,    __VA_ARGS__)                              \
    xxx(UncompressedWrite_Throughput,__VA_ARGS__)                              \
    xxx(CompressedWrite_Throughput,  __VA_ARGS__)                              \
    xxx(CompactionByReadStats_Throughput,             __VA_ARGS__)             \
    xxx(CompactionByBlobCountPerRange_Throughput,     __VA_ARGS__)             \
    xxx(CompactionByBlobCountPerDisk_Throughput,      __VA_ARGS__)             \
    xxx(CompactionByGarbageBlocksPerRange_Throughput, __VA_ARGS__)             \
    xxx(CompactionByGarbageBlocksPerDisk_Throughput,  __VA_ARGS__)             \
// YDB_CUMULATIVE_COUNTERS

#define YDB_DEFINE_CUMULATIVE_COUNTER(name, ...)                               \
    YDB_CUMULATIVE_FIELD(name.Max)                                             \
    YDB_CUMULATIVE_FIELD(name.Min)                                             \
    YDB_CUMULATIVE_FIELD(name.Avg)                                             \
// YDB_DEFINE_CUMULATIVE_COUNTER

#define YDB_SET_CUMULATIVE_COUNTER(name, ...)                                  \
    YDB_SET_CUMULATIVE_FIELD(Max_##name, name.Max)                             \
    YDB_SET_CUMULATIVE_FIELD(Min_##name, name.Min)                             \
    YDB_SET_CUMULATIVE_FIELD(Avg_##name, name.Avg)                             \
// YDB_SET_CUMULATIVE_COUNTER

#define YDB_DEFINE_CUMULATIVE_COUNTER_STRING(name, ...)                        \
    YDB_CUMULATIVE_FIELD(Max_##name)                                           \
    YDB_CUMULATIVE_FIELD(Min_##name)                                           \
    YDB_CUMULATIVE_FIELD(Avg_##name)                                           \
// YDB_DEFINE_CUMULATIVE_COUNTER_STRING

#define YDB_PERCENTILE_COUNTERS(xxx, ...)                                      \
    xxx(ReadBlocksLarge,             __VA_ARGS__)                              \
    xxx(WriteBlocksLarge,            __VA_ARGS__)                              \
    xxx(Flush,                       __VA_ARGS__)                              \
    xxx(AddBlobs,                    __VA_ARGS__)                              \
    xxx(Compaction,                  __VA_ARGS__)                              \
    xxx(Cleanup,                     __VA_ARGS__)                              \
    xxx(CollectGarbage,              __VA_ARGS__)                              \
    xxx(DeleteGarbage,               __VA_ARGS__)                              \
    xxx(WriteBlobLarge,              __VA_ARGS__)                              \
    xxx(ReadBlobLarge,               __VA_ARGS__)                              \
    xxx(WriteBlobSmall,              __VA_ARGS__)                              \
    xxx(ReadBlobSmall,               __VA_ARGS__)                              \
    xxx(ReadBlocksSmall ,            __VA_ARGS__)                              \
    xxx(WriteBlocksSmall,            __VA_ARGS__)                              \
    xxx(ZeroBlocksSmall,             __VA_ARGS__)                              \
    xxx(ZeroBlocksLarge,             __VA_ARGS__)                              \
    xxx(ReadBlocksThrottlerDelay,    __VA_ARGS__)                              \
    xxx(WriteBlocksThrottlerDelay,   __VA_ARGS__)                              \
    xxx(ZeroBlocksThrottlerDelay,    __VA_ARGS__)                              \
// YDB_PERCENTILE_COUNTERS

#define YDB_DEFINE_PERCENTILES_COUNTER(name, ...)                              \
    YDB_PERCENTILE_FIELD(name.P50)                                             \
    YDB_PERCENTILE_FIELD(name.P90)                                             \
    YDB_PERCENTILE_FIELD(name.P99)                                             \
    YDB_PERCENTILE_FIELD(name.P999)                                            \
    YDB_PERCENTILE_FIELD(name.P100)                                            \
// YDB_DEFINE_PERCENTILES_COUNTER

#define YDB_SET_PERCENTILES_COUNTER(name, ...)                                \
    YDB_SET_PERCENTILE_FIELD(name##_percentile_50, name.P50)                  \
    YDB_SET_PERCENTILE_FIELD(name##_percentile_90, name.P90)                  \
    YDB_SET_PERCENTILE_FIELD(name##_percentile_99, name.P99)                  \
    YDB_SET_PERCENTILE_FIELD(name##_percentile_99_9, name.P999)               \
    YDB_SET_PERCENTILE_FIELD(name##_percentile_100, name.P100)                \
// YDB_SET_PERCENTILES_COUNTER

#define YDB_DEFINE_PERCENTILES_COUNTER_STRING(name, ...)                       \
    YDB_PERCENTILE_FIELD(name##_percentile_50)                                 \
    YDB_PERCENTILE_FIELD(name##_percentile_90)                                 \
    YDB_PERCENTILE_FIELD(name##_percentile_99)                                 \
    YDB_PERCENTILE_FIELD(name##_percentile_99_9)                               \
    YDB_PERCENTILE_FIELD(name##_percentile_100)                                \
// YDB_DEFINE_PERCENTILES_COUNTER_STRING

////////////////////////////////////////////////////////////////////////////////

struct TCumulativeCounterField
{
    ui64 Max = 0;
    ui64 Min = 0;
    ui64 Avg = 0;
};

struct TPercentileCounterField
{
    double P50 = 0;
    double P90 = 0;
    double P99 = 0;
    double P999 = 0;
    double P100 = 0;
};

struct TYdbRow
{
#define YDB_SIMPLE_STRING_FIELD(name, ...)                                     \
    TString name;                                                              \
// YDB_SIMPLE_STRING_LABEL

    YDB_SIMPLE_STRING_COUNTERS(YDB_SIMPLE_STRING_FIELD)

#undef YDB_SIMPLE_STRING_FIELD

#define YDB_SIMPLE_UINT64_FIELD(name, ...)                                     \
    ui64 name = 0;                                                             \
// YDB_DEFINE_SIMPLE_UINT64_LABEL

    YDB_SIMPLE_UINT64_COUNTERS(YDB_SIMPLE_UINT64_FIELD)

#undef YDB_SIMPLE_UINT64_FIELD

#define YDB_CUMULATIVE_FIELD(name, ...)                                        \
    TCumulativeCounterField name;                                              \
// YDB_CUMULATIVE_LABEL

    YDB_CUMULATIVE_COUNTERS(YDB_CUMULATIVE_FIELD)
#undef YDB_CUMULATIVE_FIELD

#define YDB_PERCENTILE_FIELD(name,  ...)                                       \
    TPercentileCounterField name;                                              \
// YDB_DEFINE_PERCENTILE

    YDB_PERCENTILE_COUNTERS(YDB_PERCENTILE_FIELD)
#undef YDB_PERCENTILE_FIELD

    NYdb::TValue GetYdbValues() const;
    static TStringBuf GetYdbRowDefinition();
};

struct TYdbBlobLoadMetricRow
{
    static constexpr TStringBuf HostNameName  = "HostName";
    static constexpr TStringBuf TimestampName = "Timestamp";
    static constexpr TStringBuf LoadDataName  = "LoadData";
    static constexpr TDuration TtlDuration  = TDuration::Days(7);

    TString HostName;
    TInstant Timestamp;
    TString LoadData;

    NYdb::TValue GetYdbValues() const;
    static TStringBuf GetYdbRowDefinition();
};

struct TYdbGroupsInfoRow
{
    static constexpr TStringBuf PartitionTabletIdName = "PartitionTabletId";
    static constexpr TStringBuf ChannelName = "Channel";
    static constexpr TStringBuf GroupIdName = "GroupId";
    static constexpr TStringBuf GenerationName = "Generation";
    static constexpr TStringBuf TimestampName = "VolumeId";
    // TODO:_ what about channel type or/and storage pool type?
    // They should be already present in schemeshard...
    static constexpr TDuration TtlDuration  = TDuration::Days(7);

    ui64 PartitionTabletId;
    ui32 Channel;
    ui32 GroupId;
    ui32 Generation; // TODO:_ or just bool isWritable?
    TInstant Timestamp;  // TODO:_ or ui64?

    NYdb::TValue GetYdbValues() const;
};

struct TYdbPartitionsRow
{
    static constexpr TStringBuf DiskIdName = "DiskId";
    static constexpr TStringBuf VolumeTabletIdName = "VolumeTabletId";
    static constexpr TStringBuf PartitionTabletIdName = "PartitionTabletId";
    static constexpr TStringBuf TimestampName = "VolumeId";
    static constexpr TDuration TtlDuration  = TDuration::Days(7);

    TString DiskId;
    ui64 VolumeTabletId;
    ui64 PartitionTabletId;
    TInstant Timestamp;  // TODO:_ or ui64?

    NYdb::TValue GetYdbValues() const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NYdbStats


