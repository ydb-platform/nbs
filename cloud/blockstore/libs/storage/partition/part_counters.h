#pragma once

#include "public.h"

#include "part_events_private.h"
#include "part_tx.h"

#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/core/tablet_counters.h>

#include <ydb/core/tablet/tablet_counters_protobuf.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_IO_COUNTERS_(xxx, ...)                            \
    xxx(RequestsCount,                  __VA_ARGS__)                           \
    xxx(BlocksCount,                    __VA_ARGS__)                           \
    xxx(ExecTime,                       __VA_ARGS__)                           \
    xxx(WaitTime,                       __VA_ARGS__)                           \
// BLOCKSTORE_PARTITION_IO_COUNTERS_

#define BLOCKSTORE_PARTITION_IO_COUNTERS(xxx)                                  \
    BLOCKSTORE_PARTITION_IO_COUNTERS_(xxx, UserRead)                           \
    BLOCKSTORE_PARTITION_IO_COUNTERS_(xxx, UserWrite)                          \
    BLOCKSTORE_PARTITION_IO_COUNTERS_(xxx, SysRead)                            \
    BLOCKSTORE_PARTITION_IO_COUNTERS_(xxx, SysWrite)                           \
    BLOCKSTORE_PARTITION_IO_COUNTERS_(xxx, RealSysRead)                        \
    BLOCKSTORE_PARTITION_IO_COUNTERS_(xxx, RealSysWrite)                       \
// BLOCKSTORE_PARTITION_IO_COUNTERS

#define BLOCKSTORE_PARTITION_SIMPLE_COUNTERS(xxx)                              \
// BLOCKSTORE_PARTITION_SIMPLE_COUNTERS

#define BLOCKSTORE_PARTITION_CUMULATIVE_COUNTERS(xxx)                          \
    BLOCKSTORE_PARTITION_IO_COUNTERS(xxx)                                      \
// BLOCKSTORE_PARTITION_CUMULATIVE_COUNTERS

#define BLOCKSTORE_PARTITION_PERCENTILE_COUNTERS(xxx)                          \
// BLOCKSTORE_PARTITION_PERCENTILE_COUNTERS

////////////////////////////////////////////////////////////////////////////////

struct TPartitionCounters
{
    enum ESimpleCounter
    {
#define BLOCKSTORE_SIMPLE_COUNTER(name, category, ...) \
    SIMPLE_COUNTER_##category##_##name,

        BLOCKSTORE_PARTITION_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
        SIMPLE_COUNTER_SIZE

#undef BLOCKSTORE_SIMPLE_COUNTER
    };
    static const char* const SimpleCounterNames[SIMPLE_COUNTER_SIZE];

    enum ECumulativeCounter
    {
#define BLOCKSTORE_CUMULATIVE_COUNTER(name, category, ...) \
    CUMULATIVE_COUNTER_##category##_##name,

        BLOCKSTORE_PARTITION_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
        CUMULATIVE_COUNTER_SIZE

#undef BLOCKSTORE_CUMULATIVE_COUNTER
    };
    static const char* const CumulativeCounterNames[CUMULATIVE_COUNTER_SIZE];

    enum EPercentileCounter
    {
#define BLOCKSTORE_PERCENTILE_COUNTER(name, category, ...) \
    PERCENTILE_COUNTER_##category##_##name,

        BLOCKSTORE_PARTITION_PERCENTILE_COUNTERS(BLOCKSTORE_PERCENTILE_COUNTER)
        PERCENTILE_COUNTER_SIZE

#undef BLOCKSTORE_PERCENTILE_COUNTER
    };
    static const char* const PercentileCounterNames[PERCENTILE_COUNTER_SIZE];

    enum ETransactionType
    {
#define BLOCKSTORE_TRANSACTION_TYPE(name, ...)      TX_##name,

        BLOCKSTORE_PARTITION_TRANSACTIONS(BLOCKSTORE_TRANSACTION_TYPE)
        TX_SIZE

#undef BLOCKSTORE_TRANSACTION_TYPE
    };
    static const char* const TransactionTypeNames[TX_SIZE];
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NKikimr::TTabletCountersWithTxTypes> CreatePartitionCounters();

template <typename TCounters>
void UpdatePartitionCounters(TCounters& dst, const NProto::TPartitionStats& src);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
