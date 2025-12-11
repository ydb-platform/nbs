#include "tablet_counters.h"

#include <util/generic/singleton.h>

namespace NCloud::NFileStore::NStorage {

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TMessage, typename TExtensionIdentifier>
auto GetExtension(const TMessage& message, const TExtensionIdentifier& id)
{
    return message.HasExtension(id) ? &message.GetExtension(id) : nullptr;
}

TRangeDefs ParseRanges(const TCounterOptions* counterOpts)
{
    TRangeDefs ranges;

    if (counterOpts) {
        ranges.reserve(counterOpts->RangesSize());

        for (const auto& range: counterOpts->GetRanges()) {
            ranges.push_back({range.GetValue(), range.GetName().data()});
        }
    }

    return ranges;
}

TTxCountersDesc ParseTxCounters(const NProtoBuf::EnumDescriptor* enumDesc)
{
    TTxCountersDesc desc;

    const auto* globalCounterOpts =
        GetExtension(enumDesc->options(), GlobalCounterOpts);
    const auto globalRanges = ParseRanges(globalCounterOpts);

    for (int i = 0; i < enumDesc->value_count(); ++i) {
        const auto* valueDesc = enumDesc->value(i);

        const auto* counterOpts =
            GetExtension(valueDesc->options(), CounterOpts);
        if (!counterOpts) {
            // Values without CounterOpts should not be reported, but we still
            // need to occupy an array slot. Mark with an empty name for later
            // disambiguation.
            desc.Names.emplace_back();
            desc.Ranges.emplace_back();
            continue;
        }

        desc.Names.push_back(counterOpts->GetName());

        const auto ranges = ParseRanges(counterOpts);
        if (ranges) {
            desc.Ranges.push_back(ranges);
        } else {
            desc.Ranges.push_back(globalRanges);
        }
    }

    return desc;
}

////////////////////////////////////////////////////////////////////////////////

class TTabletCountersImpl final: public TTabletCountersBase
{
public:
    TTabletCountersImpl(
        ui32 simpleCounters,
        ui32 cumulativeCounters,
        ui32 percentileCounters,
        const char* const simpleCounterNames[],
        const char* const cumulativeCounterNames[],
        const char* const percentileCounterNames[])
        : TTabletCountersBase(
              simpleCounters,
              cumulativeCounters,
              percentileCounters,
              simpleCounterNames,
              cumulativeCounterNames,
              percentileCounterNames)
    {
        const auto& rangeDefs = DefaultRangeDefs();
        for (size_t i = 0; i < percentileCounters; ++i) {
            Percentile()[i].Initialize(
                rangeDefs.size(),
                rangeDefs.begin(),
                false);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTabletCountersWithTxTypesImpl final: public TTabletCountersWithTxTypes
{
public:
    TTabletCountersWithTxTypesImpl(
        const TTabletCountersDesc& simpleCounters,
        const TTabletCountersDesc& cumulativeCounters,
        const TTabletCountersDesc& percentileCounters)
        : TTabletCountersWithTxTypes(
              simpleCounters.Size,
              cumulativeCounters.Size,
              percentileCounters.Size,
              simpleCounters.NamePtrs.begin(),
              cumulativeCounters.NamePtrs.begin(),
              percentileCounters.NamePtrs.begin())
    {
        InitCounters(CT_SIMPLE, simpleCounters);
        InitCounters(CT_CUMULATIVE, cumulativeCounters);
        InitCounters(CT_PERCENTILE, percentileCounters);

        for (size_t i = 0; i < percentileCounters.Size; ++i) {
            if (!percentileCounters.NamePtrs[i]) {
                continue;
            }
            const auto& range = percentileCounters.Ranges[i];
            Percentile()[i].Initialize(range.size(), range.begin(), false);
        }
    }

private:
    void InitCounters(int index, const TTabletCountersDesc& counters)
    {
        Size[index] = counters.Size;
        TxOffset[index] = counters.TxOffset;
        TxCountersSize[index] = counters.TxCountersSize;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

const TRangeDefs& DefaultRangeDefs()
{
    struct TInitializer
    {
        TRangeDefs RangeDefs = {{
            {0, "0"},
            {100, "100"},
            {200, "200"},
            {500, "500"},
            {1000, "1000"},
            {2000, "2000"},
            {5000, "5000"},
            {10000, "10000"},
            {20000, "20000"},
            {50000, "50000"},
            {100000, "100000"},
            {200000, "200000"},
            {500000, "500000"},
            {1000000, "1000000"},
            {2000000, "2000000"},
            {5000000, "5000000"},
            {10000000, "10000000"},
            {35000000, "35000000"},
        }};
    };
    return Singleton<TInitializer>()->RangeDefs;
}

void IncrementFor(TTabletPercentileCounter& counter, ui64 value)
{
    for (size_t i = 0; i < counter.GetRangeCount(); ++i) {
        if (value <= counter.GetRangeBound(i)) {
            counter.IncrementForRange(i);
            break;
        }
    }
}

const TTxCountersDesc& TxSimpleCounters()
{
    struct TInitializer
    {
        TTxCountersDesc Counters =
            ParseTxCounters(ETxTypeSimpleCounters_descriptor());
    };
    return Singleton<TInitializer>()->Counters;
}

const TTxCountersDesc& TxCumulativeCounters()
{
    struct TInitializer
    {
        TTxCountersDesc Counters =
            ParseTxCounters(ETxTypeCumulativeCounters_descriptor());
    };
    return Singleton<TInitializer>()->Counters;
}

const TTxCountersDesc& TxPercentileCounters()
{
    struct TInitializer
    {
        TTxCountersDesc Counters =
            ParseTxCounters(ETxTypePercentileCounters_descriptor());
    };
    return Singleton<TInitializer>()->Counters;
}

TTabletCountersDesc BuildTabletCounters(
    size_t counters,
    const char* const counterNames[],
    size_t txTypes,
    const char* const txTypeNames[],
    const TTxCountersDesc& txCounters)
{
    TTabletCountersDesc desc;

    for (size_t i = 0; i < counters; ++i) {
        desc.Names.push_back(counterNames[i]);
        desc.Ranges.push_back(DefaultRangeDefs());
    }

    for (size_t i = 0; i < txTypes; ++i) {
        auto txPrefix = TString(txTypeNames[i]) + "/";
        for (size_t j = 0; j < txCounters.Names.size(); ++j) {
            if (txCounters.Names[j].empty()) {
                // Allocate array slot, but keep an empty name as a marker
                desc.Names.emplace_back();
                desc.Ranges.emplace_back();
                continue;
            }

            desc.Names.push_back(txPrefix + txCounters.Names[j]);
            desc.Ranges.push_back(txCounters.Ranges[j]);
        }
    }

    for (const auto& name: desc.Names) {
        // Set name pointer to nullptr for array slots that should not be
        // reported
        desc.NamePtrs.push_back(name.empty() ? nullptr : name.data());
    }

    desc.Size = desc.Names.size();
    desc.TxOffset = counters;
    desc.TxCountersSize = txCounters.Names.size();

    return desc;
}

std::unique_ptr<TTabletCountersBase> CreateTabletCounters(
    ui32 simpleCounters,
    ui32 cumulativeCounters,
    ui32 percentileCounters,
    const char* const simpleCounterNames[],
    const char* const cumulativeCounterNames[],
    const char* const percentileCounterNames[])
{
    return std::make_unique<TTabletCountersImpl>(
        simpleCounters,
        cumulativeCounters,
        percentileCounters,
        simpleCounterNames,
        cumulativeCounterNames,
        percentileCounterNames);
}

std::unique_ptr<TTabletCountersWithTxTypes> CreateTabletCountersWithTxTypes(
    const TTabletCountersDesc& simpleCounters,
    const TTabletCountersDesc& cumulativeCounters,
    const TTabletCountersDesc& percentileCounters)
{
    return std::make_unique<TTabletCountersWithTxTypesImpl>(
        simpleCounters,
        cumulativeCounters,
        percentileCounters);
}

}   // namespace NCloud::NFileStore::NStorage
