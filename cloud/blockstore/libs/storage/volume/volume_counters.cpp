#include "volume_counters.h"

#include <util/generic/singleton.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_COUNTER_NAME(name, category, ...) #category "/" #name,

const char* const TVolumeCounters::SimpleCounterNames[] = {
    BLOCKSTORE_VOLUME_SIMPLE_COUNTERS(BLOCKSTORE_COUNTER_NAME)};

const char* const TVolumeCounters::CumulativeCounterNames[] = {
    BLOCKSTORE_VOLUME_CUMULATIVE_COUNTERS(BLOCKSTORE_COUNTER_NAME)};

const char* const TVolumeCounters::PercentileCounterNames[] = {
    BLOCKSTORE_VOLUME_PERCENTILE_COUNTERS(BLOCKSTORE_COUNTER_NAME)};

const char* const TVolumeCounters::TransactionTypeNames[] = {
    BLOCKSTORE_VOLUME_TRANSACTIONS(BLOCKSTORE_COUNTER_NAME, Tx)};

#undef BLOCKSTORE_COUNTER_NAME

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TTabletCountersWithTxTypes> CreateVolumeCounters()
{
    struct TInitializer
    {
        TTabletCountersDesc SimpleCounters = BuildTabletCounters(
            TVolumeCounters::SIMPLE_COUNTER_SIZE,
            TVolumeCounters::SimpleCounterNames,
            TVolumeCounters::TX_SIZE,
            TVolumeCounters::TransactionTypeNames,
            TxSimpleCounters());

        TTabletCountersDesc CumulativeCounters = BuildTabletCounters(
            TVolumeCounters::CUMULATIVE_COUNTER_SIZE,
            TVolumeCounters::CumulativeCounterNames,
            TVolumeCounters::TX_SIZE,
            TVolumeCounters::TransactionTypeNames,
            TxCumulativeCounters());

        TTabletCountersDesc PercentileCounters = BuildTabletCounters(
            TVolumeCounters::PERCENTILE_COUNTER_SIZE,
            TVolumeCounters::PercentileCounterNames,
            TVolumeCounters::TX_SIZE,
            TVolumeCounters::TransactionTypeNames,
            TxPercentileCounters());
    };

    const auto& initializer = Default<TInitializer>();

    return CreateTabletCountersWithTxTypes(
        initializer.SimpleCounters,
        initializer.CumulativeCounters,
        initializer.PercentileCounters);
}

}   // namespace NCloud::NBlockStore::NStorage
