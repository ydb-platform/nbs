#include "service_counters.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_COUNTER_NAME(name, category, ...) #category "/" #name,

const char* const TServiceCounters::SimpleCounterNames[] = {
    BLOCKSTORE_SERVICE_SIMPLE_COUNTERS(BLOCKSTORE_COUNTER_NAME)};

const char* const TServiceCounters::CumulativeCounterNames[] = {
    BLOCKSTORE_SERVICE_CUMULATIVE_COUNTERS(BLOCKSTORE_COUNTER_NAME)};

const char* const TServiceCounters::PercentileCounterNames[] = {
    BLOCKSTORE_SERVICE_PERCENTILE_COUNTERS(BLOCKSTORE_COUNTER_NAME)};

#undef BLOCKSTORE_COUNTER_NAME

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TTabletCountersBase> CreateServiceCounters()
{
    return CreateTabletCounters(
        TServiceCounters::SIMPLE_COUNTER_SIZE,
        TServiceCounters::CUMULATIVE_COUNTER_SIZE,
        TServiceCounters::PERCENTILE_COUNTER_SIZE,
        TServiceCounters::SimpleCounterNames,
        TServiceCounters::CumulativeCounterNames,
        TServiceCounters::PercentileCounterNames);
}

}   // namespace NCloud::NBlockStore::NStorage
