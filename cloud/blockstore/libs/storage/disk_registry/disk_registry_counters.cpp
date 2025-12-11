#include "disk_registry_counters.h"

#include <util/generic/singleton.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_COUNTER_NAME(name, category, ...) #category "/" #name,

const char* const TDiskRegistryCounters::SimpleCounterNames[] = {
    BLOCKSTORE_DISK_REGISTRY_SIMPLE_COUNTERS(BLOCKSTORE_COUNTER_NAME)};

const char* const TDiskRegistryCounters::CumulativeCounterNames[] = {
    BLOCKSTORE_DISK_REGISTRY_CUMULATIVE_COUNTERS(BLOCKSTORE_COUNTER_NAME)};

const char* const TDiskRegistryCounters::PercentileCounterNames[] = {
    BLOCKSTORE_DISK_REGISTRY_PERCENTILE_COUNTERS(BLOCKSTORE_COUNTER_NAME)};

const char* const TDiskRegistryCounters::TransactionTypeNames[] = {
    BLOCKSTORE_DISK_REGISTRY_TRANSACTIONS(BLOCKSTORE_COUNTER_NAME, Tx)};

#undef BLOCKSTORE_COUNTER_NAME

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TTabletCountersWithTxTypes> CreateDiskRegistryCounters()
{
    struct TInitializer
    {
        TTabletCountersDesc SimpleCounters = BuildTabletCounters(
            TDiskRegistryCounters::SIMPLE_COUNTER_SIZE,
            TDiskRegistryCounters::SimpleCounterNames,
            TDiskRegistryCounters::TX_SIZE,
            TDiskRegistryCounters::TransactionTypeNames,
            TxSimpleCounters());

        TTabletCountersDesc CumulativeCounters = BuildTabletCounters(
            TDiskRegistryCounters::CUMULATIVE_COUNTER_SIZE,
            TDiskRegistryCounters::CumulativeCounterNames,
            TDiskRegistryCounters::TX_SIZE,
            TDiskRegistryCounters::TransactionTypeNames,
            TxCumulativeCounters());

        TTabletCountersDesc PercentileCounters = BuildTabletCounters(
            TDiskRegistryCounters::PERCENTILE_COUNTER_SIZE,
            TDiskRegistryCounters::PercentileCounterNames,
            TDiskRegistryCounters::TX_SIZE,
            TDiskRegistryCounters::TransactionTypeNames,
            TxPercentileCounters());
    };

    const auto& initializer = Default<TInitializer>();

    return CreateTabletCountersWithTxTypes(
        initializer.SimpleCounters,
        initializer.CumulativeCounters,
        initializer.PercentileCounters);
}

}   // namespace NCloud::NBlockStore::NStorage
