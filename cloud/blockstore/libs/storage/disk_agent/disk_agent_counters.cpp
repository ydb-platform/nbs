#include "disk_agent_counters.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_COUNTER_NAME(name, category, ...) #category "/" #name,

const char* const TDiskAgentCounters::SimpleCounterNames[] = {
    BLOCKSTORE_DISK_AGENT_SIMPLE_COUNTERS(BLOCKSTORE_COUNTER_NAME)};

const char* const TDiskAgentCounters::CumulativeCounterNames[] = {
    BLOCKSTORE_DISK_AGENT_CUMULATIVE_COUNTERS(BLOCKSTORE_COUNTER_NAME)};

const char* const TDiskAgentCounters::PercentileCounterNames[] = {
    BLOCKSTORE_DISK_AGENT_PERCENTILE_COUNTERS(BLOCKSTORE_COUNTER_NAME)};

#undef BLOCKSTORE_COUNTER_NAME

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TTabletCountersBase> CreateDiskAgentCounters()
{
    return CreateTabletCounters(
        TDiskAgentCounters::SIMPLE_COUNTER_SIZE,
        TDiskAgentCounters::CUMULATIVE_COUNTER_SIZE,
        TDiskAgentCounters::PERCENTILE_COUNTER_SIZE,
        TDiskAgentCounters::SimpleCounterNames,
        TDiskAgentCounters::CumulativeCounterNames,
        TDiskAgentCounters::PercentileCounterNames);
}

}   // namespace NCloud::NBlockStore::NStorage
