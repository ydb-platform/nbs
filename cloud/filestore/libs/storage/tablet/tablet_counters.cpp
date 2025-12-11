#include "tablet_counters.h"

#include <util/generic/singleton.h>

namespace NCloud::NFileStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_COUNTER_NAME(name, category, ...) #category "/" #name,

const char* const TIndexTabletCounters::SimpleCounterNames[] = {
    FILESTORE_TABLET_SIMPLE_COUNTERS(FILESTORE_COUNTER_NAME)};

const char* const TIndexTabletCounters::CumulativeCounterNames[] = {
    FILESTORE_TABLET_CUMULATIVE_COUNTERS(FILESTORE_COUNTER_NAME)};

const char* const TIndexTabletCounters::PercentileCounterNames[] = {
    FILESTORE_TABLET_PERCENTILE_COUNTERS(FILESTORE_COUNTER_NAME)};

const char* const TIndexTabletCounters::TransactionTypeNames[] = {
    FILESTORE_TABLET_TRANSACTIONS(FILESTORE_COUNTER_NAME, Tx)};

#undef FILESTORE_COUNTER_NAME

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TTabletCountersWithTxTypes> CreateIndexTabletCounters()
{
    struct TInitializer
    {
        TTabletCountersDesc SimpleCounters = BuildTabletCounters(
            TIndexTabletCounters::SIMPLE_COUNTER_SIZE,
            TIndexTabletCounters::SimpleCounterNames,
            TIndexTabletCounters::TX_SIZE,
            TIndexTabletCounters::TransactionTypeNames,
            TxSimpleCounters());

        TTabletCountersDesc CumulativeCounters = BuildTabletCounters(
            TIndexTabletCounters::CUMULATIVE_COUNTER_SIZE,
            TIndexTabletCounters::CumulativeCounterNames,
            TIndexTabletCounters::TX_SIZE,
            TIndexTabletCounters::TransactionTypeNames,
            TxCumulativeCounters());

        TTabletCountersDesc PercentileCounters = BuildTabletCounters(
            TIndexTabletCounters::PERCENTILE_COUNTER_SIZE,
            TIndexTabletCounters::PercentileCounterNames,
            TIndexTabletCounters::TX_SIZE,
            TIndexTabletCounters::TransactionTypeNames,
            TxPercentileCounters());
    };

    const auto& initializer = Default<TInitializer>();

    return CreateTabletCountersWithTxTypes(
        initializer.SimpleCounters,
        initializer.CumulativeCounters,
        initializer.PercentileCounters);
}

}   // namespace NCloud::NFileStore::NStorage
