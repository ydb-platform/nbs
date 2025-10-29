#pragma once

#include "public.h"

#include "disk_agent_private.h"

#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/tablet_counters.h>

#include <ydb/core/tablet/tablet_counters.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DISK_AGENT_ACTOR_COUNTERS(xxx, ...)                         \
    xxx(ActorQueue,                     __VA_ARGS__)                           \
    xxx(MailboxQueue,                   __VA_ARGS__)                           \
// BLOCKSTORE_DISK_AGENT_ACTOR_COUNTERS

#define BLOCKSTORE_DISK_AGENT_SIMPLE_COUNTERS(xxx)                             \
// BLOCKSTORE_DISK_AGENT_SIMPLE_COUNTERS

#define BLOCKSTORE_DISK_AGENT_CUMULATIVE_COUNTERS(xxx)                         \
    BLOCKSTORE_DISK_AGENT_REQUESTS(xxx, Request)                               \
    BLOCKSTORE_DISK_AGENT_REQUESTS_PRIVATE(xxx, Request)                       \
// BLOCKSTORE_DISK_AGENT_CUMULATIVE_COUNTERS

#define BLOCKSTORE_DISK_AGENT_PERCENTILE_COUNTERS(xxx)                         \
    BLOCKSTORE_DISK_AGENT_ACTOR_COUNTERS(xxx, Actor)                           \
// BLOCKSTORE_DISK_AGENT_PERCENTILE_COUNTERS

////////////////////////////////////////////////////////////////////////////////

struct TDiskAgentCounters
{
    enum ESimpleCounter
    {
#define BLOCKSTORE_SIMPLE_COUNTER(name, category, ...) \
    SIMPLE_COUNTER_##category##_##name,

        BLOCKSTORE_DISK_AGENT_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
        SIMPLE_COUNTER_SIZE

#undef BLOCKSTORE_SIMPLE_COUNTER
    };
    static const char* const SimpleCounterNames[SIMPLE_COUNTER_SIZE];

    enum ECumulativeCounter
    {
#define BLOCKSTORE_CUMULATIVE_COUNTER(name, category, ...) \
    CUMULATIVE_COUNTER_##category##_##name,

        BLOCKSTORE_DISK_AGENT_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
        CUMULATIVE_COUNTER_SIZE

#undef BLOCKSTORE_CUMULATIVE_COUNTER
    };
    static const char* const CumulativeCounterNames[CUMULATIVE_COUNTER_SIZE];

    enum EPercentileCounter
    {
#define BLOCKSTORE_PERCENTILE_COUNTER(name, category, ...) \
    PERCENTILE_COUNTER_##category##_##name,

        BLOCKSTORE_DISK_AGENT_PERCENTILE_COUNTERS(BLOCKSTORE_PERCENTILE_COUNTER)
        PERCENTILE_COUNTER_SIZE

#undef BLOCKSTORE_PERCENTILE_COUNTER
    };
    static const char* const PercentileCounterNames[PERCENTILE_COUNTER_SIZE];
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NKikimr::TTabletCountersBase> CreateDiskAgentCounters();

}   // namespace NCloud::NBlockStore::NStorage
