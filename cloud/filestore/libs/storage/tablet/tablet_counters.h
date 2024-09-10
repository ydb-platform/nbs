#pragma once

#include "public.h"

#include "tablet_tx.h"

#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/core/tablet_counters.h>
#include <cloud/filestore/libs/storage/model/block_buffer.h>

#include <ydb/core/tablet/tablet_counters_protobuf.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_TABLET_STATS(xxx, ...)                                       \
    xxx(UsedNodesCount,         __VA_ARGS__)                                   \
    xxx(UsedSessionsCount,      __VA_ARGS__)                                   \
    xxx(UsedHandlesCount,       __VA_ARGS__)                                   \
    xxx(UsedLocksCount,         __VA_ARGS__)                                   \
    xxx(UsedBlocksCount,        __VA_ARGS__)                                   \
                                                                               \
    xxx(FreshBlocksCount,           __VA_ARGS__)                               \
    xxx(MixedBlocksCount,           __VA_ARGS__)                               \
    xxx(MixedBlobsCount,            __VA_ARGS__)                               \
    xxx(DeletionMarkersCount,       __VA_ARGS__)                               \
    xxx(GarbageQueueSize,           __VA_ARGS__)                               \
    xxx(GarbageBlocksCount,         __VA_ARGS__)                               \
    xxx(CheckpointNodesCount,       __VA_ARGS__)                               \
    xxx(CheckpointBlocksCount,      __VA_ARGS__)                               \
    xxx(CheckpointBlobsCount,       __VA_ARGS__)                               \
    xxx(FreshBytesCount,            __VA_ARGS__)                               \
    xxx(DeletedFreshBytesCount,     __VA_ARGS__)                               \
    xxx(LastCollectCommitId,        __VA_ARGS__)                               \
    xxx(LargeDeletionMarkersCount,  __VA_ARGS__)                               \
// FILESTORE_TABLET_STATS

#define FILESTORE_TABLET_SIMPLE_COUNTERS(xxx)                                  \
    FILESTORE_TABLET_STATS(xxx, Stats)                                         \
// FILESTORE_TABLET_SIMPLE_COUNTERS

#define FILESTORE_TABLET_CUMULATIVE_COUNTERS(xxx)                              \
// FILESTORE_TABLET_CUMULATIVE_COUNTERS

#define FILESTORE_TABLET_PERCENTILE_COUNTERS(xxx)                              \
// FILESTORE_TABLET_PERCENTILE_COUNTERS

////////////////////////////////////////////////////////////////////////////////

struct TIndexTabletCounters
{
    enum ESimpleCounter
    {
#define FILESTORE_SIMPLE_COUNTER(name, category, ...) \
    SIMPLE_COUNTER_##category##_##name,

        FILESTORE_TABLET_SIMPLE_COUNTERS(FILESTORE_SIMPLE_COUNTER)
        SIMPLE_COUNTER_SIZE

#undef FILESTORE_SIMPLE_COUNTER
    };
    static const char* const SimpleCounterNames[SIMPLE_COUNTER_SIZE];

    enum ECumulativeCounter
    {
#define FILESTORE_CUMULATIVE_COUNTER(name, category, ...) \
    CUMULATIVE_COUNTER_##category##_##name,

        FILESTORE_TABLET_CUMULATIVE_COUNTERS(FILESTORE_CUMULATIVE_COUNTER)
        CUMULATIVE_COUNTER_SIZE

#undef FILESTORE_CUMULATIVE_COUNTER
    };
    static const char* const CumulativeCounterNames[CUMULATIVE_COUNTER_SIZE];

    enum EPercentileCounter
    {
#define FILESTORE_PERCENTILE_COUNTER(name, category, ...) \
    PERCENTILE_COUNTER_##category##_##name,

        FILESTORE_TABLET_PERCENTILE_COUNTERS(FILESTORE_PERCENTILE_COUNTER)
        PERCENTILE_COUNTER_SIZE

#undef FILESTORE_PERCENTILE_COUNTER
    };
    static const char* const PercentileCounterNames[PERCENTILE_COUNTER_SIZE];

    enum ETransactionType
    {
#define FILESTORE_TRANSACTION_TYPE(name, ...)      TX_##name,

        FILESTORE_TABLET_TRANSACTIONS(FILESTORE_TRANSACTION_TYPE)
        TX_SIZE

#undef FILESTORE_TRANSACTION_TYPE
    };
    static const char* const TransactionTypeNames[TX_SIZE];
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NKikimr::TTabletCountersWithTxTypes> CreateIndexTabletCounters();

}   // namespace NCloud::NFileStore::NStorage
