#include "part2_counters.h"

#include <util/generic/singleton.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define UPDATE_FIELD(l, r, name) l.Set##name(l.Get##name() + r.Get##name());

template <typename T1, typename T2>
void UpdateIOCounters(T1& l, const T2& r)
{
    UPDATE_FIELD(l, r, RequestsCount);
    UPDATE_FIELD(l, r, BatchCount);
    UPDATE_FIELD(l, r, BlocksCount);
    UPDATE_FIELD(l, r, ExecTime);
    UPDATE_FIELD(l, r, WaitTime);
}

#undef UPDATE_FIELD

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_COUNTER_NAME(name, category, ...) #category "/" #name,

const char* const TPartitionCounters::SimpleCounterNames[] = {
    BLOCKSTORE_PARTITION2_SIMPLE_COUNTERS(BLOCKSTORE_COUNTER_NAME)};

const char* const TPartitionCounters::CumulativeCounterNames[] = {
    BLOCKSTORE_PARTITION2_CUMULATIVE_COUNTERS(BLOCKSTORE_COUNTER_NAME)};

const char* const TPartitionCounters::PercentileCounterNames[] = {
    BLOCKSTORE_PARTITION2_PERCENTILE_COUNTERS(BLOCKSTORE_COUNTER_NAME)};

const char* const TPartitionCounters::TransactionTypeNames[] = {
    BLOCKSTORE_PARTITION2_TRANSACTIONS(BLOCKSTORE_COUNTER_NAME, Tx)};

#undef BLOCKSTORE_COUNTER_NAME

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TTabletCountersWithTxTypes> CreatePartitionCounters()
{
    struct TInitializer
    {
        TTabletCountersDesc SimpleCounters = BuildTabletCounters(
            TPartitionCounters::SIMPLE_COUNTER_SIZE,
            TPartitionCounters::SimpleCounterNames,
            TPartitionCounters::TX_SIZE,
            TPartitionCounters::TransactionTypeNames,
            TxSimpleCounters());

        TTabletCountersDesc CumulativeCounters = BuildTabletCounters(
            TPartitionCounters::CUMULATIVE_COUNTER_SIZE,
            TPartitionCounters::CumulativeCounterNames,
            TPartitionCounters::TX_SIZE,
            TPartitionCounters::TransactionTypeNames,
            TxCumulativeCounters());

        TTabletCountersDesc PercentileCounters = BuildTabletCounters(
            TPartitionCounters::PERCENTILE_COUNTER_SIZE,
            TPartitionCounters::PercentileCounterNames,
            TPartitionCounters::TX_SIZE,
            TPartitionCounters::TransactionTypeNames,
            TxPercentileCounters());
    };

    const auto& initializer = Default<TInitializer>();

    return CreateTabletCountersWithTxTypes(
        initializer.SimpleCounters,
        initializer.CumulativeCounters,
        initializer.PercentileCounters);
}

template <typename TCounters>
void UpdatePartitionCounters(TCounters& l, const NProto::TPartitionStats& r)
{
    if (r.HasUserReadCounters()) {
        UpdateIOCounters(*l.MutableUserReadCounters(), r.GetUserReadCounters());
    }

    if (r.HasUserWriteCounters()) {
        UpdateIOCounters(
            *l.MutableUserWriteCounters(),
            r.GetUserWriteCounters());
    }

    if (r.HasSysReadCounters()) {
        UpdateIOCounters(*l.MutableSysReadCounters(), r.GetSysReadCounters());
    }

    if (r.HasSysWriteCounters()) {
        UpdateIOCounters(*l.MutableSysWriteCounters(), r.GetSysWriteCounters());
    }
}

template void UpdatePartitionCounters(
    NProto::TPartitionStats& dst,
    const NProto::TPartitionStats& src);

template void UpdatePartitionCounters(
    NProto::TVolumeStats& dst,
    const NProto::TPartitionStats& src);

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
