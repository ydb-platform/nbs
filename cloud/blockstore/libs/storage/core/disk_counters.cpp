#include "disk_counters.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TPartitionDiskCounters::Add(const TPartitionDiskCounters& source)
{
#define BLOCKSTORE_SIMPLE_COUNTER(name, ...)                                   \
    Simple.name.Add(source.Simple.name);                                       \
// BLOCKSTORE_SIMPLE_COUNTER

    BLOCKSTORE_PART_COMMON_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    BLOCKSTORE_REPL_PART_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    BLOCKSTORE_DRBASED_PART_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
#undef BLOCKSTORE_SIMPLE_COUNTER

#define BLOCKSTORE_CUMULATIVE_COUNTER(name, ...)                               \
    Cumulative.name.Add(source.Cumulative.name);                               \
// BLOCKSTORE_CUMULATIVE_COUNTER

    BLOCKSTORE_DRBASED_PART_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
    BLOCKSTORE_REPL_PART_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
#undef BLOCKSTORE_CUMULATIVE_COUNTER

#define BLOCKSTORE_REQUEST_COUNTER(name, ...)                                  \
    RequestCounters.name.Add(source.RequestCounters.name);                     \
// BLOCKSTORE_REQUEST_COUNTER

    BLOCKSTORE_REPL_PART_REQUEST_COUNTERS(BLOCKSTORE_REQUEST_COUNTER)
    BLOCKSTORE_PART_REQUEST_COUNTERS_WITH_SIZE(BLOCKSTORE_REQUEST_COUNTER)
    BLOCKSTORE_REPL_PART_REQUEST_COUNTERS_WITH_SIZE_AND_KIND(BLOCKSTORE_REQUEST_COUNTER)
#undef BLOCKSTORE_REQUEST_COUNTER

#define BLOCKSTORE_HIST_COUNTER(name, ...)                                     \
    Histogram.name.Add(source.Histogram.name);                                 \
// BLOCKSTORE_HIST_COUNTER

    BLOCKSTORE_REPL_PART_HIST_COUNTERS(BLOCKSTORE_HIST_COUNTER)
#undef BLOCKSTORE_HIST_COUNTER
}

void TPartitionDiskCounters::AggregateWith(const TPartitionDiskCounters& source)
{
#define BLOCKSTORE_SIMPLE_COUNTER(name, ...)                                   \
    Simple.name.AggregateWith(source.Simple.name);                             \
// BLOCKSTORE_SIMPLE_COUNTER

    BLOCKSTORE_PART_COMMON_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    BLOCKSTORE_REPL_PART_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    BLOCKSTORE_DRBASED_PART_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
#undef BLOCKSTORE_SIMPLE_COUNTER

#define BLOCKSTORE_CUMULATIVE_COUNTER(name, ...)                               \
    Cumulative.name.AggregateWith(source.Cumulative.name);                     \
// BLOCKSTORE_CUMULATIVE_COUNTER

    BLOCKSTORE_DRBASED_PART_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
    BLOCKSTORE_REPL_PART_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
#undef BLOCKSTORE_CUMULATIVE_COUNTER

#define BLOCKSTORE_REQUEST_COUNTER(name, ...)                                  \
    RequestCounters.name.AggregateWith(source.RequestCounters.name);           \
// BLOCKSTORE_REQUEST_COUNTER

    BLOCKSTORE_REPL_PART_REQUEST_COUNTERS(BLOCKSTORE_REQUEST_COUNTER)
    BLOCKSTORE_PART_REQUEST_COUNTERS_WITH_SIZE(BLOCKSTORE_REQUEST_COUNTER)
    BLOCKSTORE_REPL_PART_REQUEST_COUNTERS_WITH_SIZE_AND_KIND(BLOCKSTORE_REQUEST_COUNTER)
#undef BLOCKSTORE_REQUEST_COUNTER

#define BLOCKSTORE_HIST_COUNTER(name, ...)                                     \
    Histogram.name.AggregateWith(source.Histogram.name);                       \
// BLOCKSTORE_HIST_COUNTER

        BLOCKSTORE_REPL_PART_HIST_COUNTERS(BLOCKSTORE_HIST_COUNTER)
#undef BLOCKSTORE_HIST_COUNTER
}

void TPartitionDiskCounters::Publish(TInstant now)
{
#define BLOCKSTORE_SIMPLE_COUNTER(name, ...)                                   \
    Simple.name.Publish(now);                                                  \
// BLOCKSTORE_SIMPLE_COUNTER

    BLOCKSTORE_PART_COMMON_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    if (Policy == EPublishingPolicy::All || Policy == EPublishingPolicy::Repl) {
        BLOCKSTORE_REPL_PART_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    }
    if (Policy == EPublishingPolicy::All
            || Policy == EPublishingPolicy::DiskRegistryBased)
    {
        BLOCKSTORE_DRBASED_PART_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    }
#undef BLOCKSTORE_SIMPLE_COUNTER

#define BLOCKSTORE_CUMULATIVE_COUNTER(name, ...)                               \
    Cumulative.name.Publish(now);                                              \
// BLOCKSTORE_CUMULATIVE_COUNTER

    if (Policy == EPublishingPolicy::All || Policy == EPublishingPolicy::Repl) {
        BLOCKSTORE_REPL_PART_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
    }
    if (Policy == EPublishingPolicy::All || Policy == EPublishingPolicy::DiskRegistryBased) {
        BLOCKSTORE_DRBASED_PART_CUMULATIVE_COUNTERS(
            BLOCKSTORE_CUMULATIVE_COUNTER)
    }
#undef BLOCKSTORE_CUMULATIVE_COUNTER

#define BLOCKSTORE_REQUEST_COUNTER(name, ...)                                  \
    RequestCounters.name.Publish();                                            \
// BLOCKSTORE_REQUEST_COUNTER

    if (Policy == EPublishingPolicy::All || Policy == EPublishingPolicy::Repl) {
        BLOCKSTORE_REPL_PART_REQUEST_COUNTERS(BLOCKSTORE_REQUEST_COUNTER)
        BLOCKSTORE_REPL_PART_REQUEST_COUNTERS_WITH_SIZE_AND_KIND(BLOCKSTORE_REQUEST_COUNTER)
    }
    BLOCKSTORE_PART_REQUEST_COUNTERS_WITH_SIZE(BLOCKSTORE_REQUEST_COUNTER)
#undef BLOCKSTORE_REQUEST_COUNTER

#define BLOCKSTORE_HIST_COUNTER(name, ...)                                     \
    Histogram.name.Publish();                                                  \
// BLOCKSTORE_HIST_COUNTER

    if (Policy == EPublishingPolicy::All || Policy == EPublishingPolicy::Repl) {
        BLOCKSTORE_REPL_PART_HIST_COUNTERS(BLOCKSTORE_HIST_COUNTER)
    }
#undef BLOCKSTORE_HIST_COUNTER

    Reset();
}

void TPartitionDiskCounters::Register(
    NMonitoring::TDynamicCountersPtr counters,
    bool aggregate)
{
    ERequestCounterOptions requestCounterOptions;
    if (aggregate) {
        requestCounterOptions =
            requestCounterOptions | ERequestCounterOption::ReportHistogram;
    }
    ERequestCounterOptions counterOptions =
        requestCounterOptions;
    ERequestCounterOptions kindCounterOptions =
        requestCounterOptions |
        ERequestCounterOption::HasKind;

#define BLOCKSTORE_SIMPLE_COUNTER(name, ...)                                   \
    Simple.name.Register(counters, #name);                                     \
// BLOCKSTORE_SIMPLE_COUNTER

    BLOCKSTORE_PART_COMMON_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    if (Policy == EPublishingPolicy::All || Policy == EPublishingPolicy::Repl) {
        BLOCKSTORE_REPL_PART_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    }
    if (Policy == EPublishingPolicy::All
            || Policy == EPublishingPolicy::DiskRegistryBased)
    {
        BLOCKSTORE_DRBASED_PART_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    }
#undef BLOCKSTORE_SIMPLE_COUNTER

#define BLOCKSTORE_CUMULATIVE_COUNTER(name, ...)                               \
Cumulative.name.Register(counters, #name);                                     \
// BLOCKSTORE_CUMULATIVE_COUNTER

    if (Policy == EPublishingPolicy::All || Policy == EPublishingPolicy::Repl) {
        BLOCKSTORE_REPL_PART_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
    }
    if (Policy == EPublishingPolicy::All ||
        Policy == EPublishingPolicy::DiskRegistryBased)
    {
        BLOCKSTORE_DRBASED_PART_CUMULATIVE_COUNTERS(
            BLOCKSTORE_CUMULATIVE_COUNTER)
    }
#undef BLOCKSTORE_CUMULATIVE_COUNTER

#define BLOCKSTORE_REQUEST_COUNTER(name, ...)                                  \
    RequestCounters.name.Register(                                             \
        counters->GetSubgroup("request", #name), requestCounterOptions);       \
//  BLOCKSTORE_REQUEST_COUNTER

    if (Policy == EPublishingPolicy::All || Policy == EPublishingPolicy::Repl) {
        BLOCKSTORE_REPL_PART_REQUEST_COUNTERS(BLOCKSTORE_REQUEST_COUNTER)
    }
#undef BLOCKSTORE_REQUEST_COUNTER

#define BLOCKSTORE_REQUEST_COUNTER(name, ...)                                  \
    RequestCounters.name.Register(                                             \
        counters->GetSubgroup("request", #name), counterOptions);              \

    BLOCKSTORE_PART_REQUEST_COUNTERS_WITH_SIZE(BLOCKSTORE_REQUEST_COUNTER)
#undef BLOCKSTORE_REQUEST_COUNTER

#define BLOCKSTORE_REQUEST_COUNTER(name, ...)                                  \
    RequestCounters.name.Register(                                             \
        counters->GetSubgroup("request", #name), kindCounterOptions);          \

    if (Policy == EPublishingPolicy::All || Policy == EPublishingPolicy::Repl) {
        BLOCKSTORE_REPL_PART_REQUEST_COUNTERS_WITH_SIZE_AND_KIND(BLOCKSTORE_REQUEST_COUNTER)
    }
#undef BLOCKSTORE_REQUEST_COUNTER


#define BLOCKSTORE_HIST_COUNTER(name, ...) \
    Histogram.name.Register(counters->GetSubgroup("queue", #name), aggregate);

    if (Policy == EPublishingPolicy::All || Policy == EPublishingPolicy::Repl) {
        BLOCKSTORE_REPL_PART_HIST_COUNTERS(BLOCKSTORE_HIST_COUNTER)
    }
#undef BLOCKSTORE_HIST_COUNTER
}

void TPartitionDiskCounters::Reset()
{
#define BLOCKSTORE_SIMPLE_COUNTER(name, ...)                                   \
    Simple.name.Reset();                                                       \
// BLOCKSTORE_SIMPLE_COUNTER

    BLOCKSTORE_PART_COMMON_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    BLOCKSTORE_REPL_PART_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    BLOCKSTORE_DRBASED_PART_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
#undef BLOCKSTORE_SIMPLE_COUNTER

#define BLOCKSTORE_CUMULATIVE_COUNTER(name, ...)                               \
    Cumulative.name.Reset();                                                   \
// BLOCKSTORE_CUMULATIVE_COUNTER

    BLOCKSTORE_REPL_PART_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
    BLOCKSTORE_DRBASED_PART_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
#undef BLOCKSTORE_CUMULATIVE_COUNTER

#define BLOCKSTORE_REQUEST_COUNTER(name, ...)                                  \
    RequestCounters.name.Reset();                                              \
// BLOCKSTORE_REQUEST_COUNTER

    BLOCKSTORE_REPL_PART_REQUEST_COUNTERS(BLOCKSTORE_REQUEST_COUNTER)
    BLOCKSTORE_PART_REQUEST_COUNTERS_WITH_SIZE(BLOCKSTORE_REQUEST_COUNTER)
    BLOCKSTORE_REPL_PART_REQUEST_COUNTERS_WITH_SIZE_AND_KIND(BLOCKSTORE_REQUEST_COUNTER)
#undef BLOCKSTORE_REQUEST_COUNTER

#define BLOCKSTORE_HIST_COUNTER(name, ...)                                     \
    Histogram.name.Reset();                                                    \
// BLOCKSTORE_HIST_COUNTER

    BLOCKSTORE_REPL_PART_HIST_COUNTERS(BLOCKSTORE_HIST_COUNTER)
#undef BLOCKSTORE_HIST_COUNTER
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeSelfCounters::AggregateWith(const TVolumeSelfCounters& source)
{
#define BLOCKSTORE_SIMPLE_COUNTER(name, ...)                                   \
    Simple.name.AggregateWith(source.Simple.name);                             \
// BLOCKSTORE_SIMPLE_COUNTER

    BLOCKSTORE_VOLUME_SELF_COMMON_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    BLOCKSTORE_REPL_VOLUME_SELF_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    BLOCKSTORE_DRBASED_VOLUME_SELF_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
#undef BLOCKSTORE_SIMPLE_COUNTER

#define BLOCKSTORE_CUMULATIVE_COUNTER(name, ...)                               \
    Cumulative.name.AggregateWith(source.Cumulative.name);                     \
//  BLOCKSTORE_CUMULATIVE_COUNTER

    BLOCKSTORE_VOLUME_SELF_COMMON_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
#undef BLOCKSTORE_CUMULATIVE_COUNTER

#define BLOCKSTORE_REQUEST_COUNTER(name, ...)                                  \
    RequestCounters.name.AggregateWith(source.RequestCounters.name);           \
// BLOCKSTORE_REQUEST_COUNTER

    BLOCKSTORE_VOLUME_SELF_COMMON_REQUEST_COUNTERS(BLOCKSTORE_REQUEST_COUNTER)
#undef BLOCKSTORE_REQUEST_COUNTER
}

void TVolumeSelfCounters::Add(const TVolumeSelfCounters& source)
{
#define BLOCKSTORE_SIMPLE_COUNTER(name, ...)                                   \
    Simple.name.Add(source.Simple.name);                                       \
// BLOCKSTORE_SIMPLE_COUNTER

    BLOCKSTORE_VOLUME_SELF_COMMON_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    BLOCKSTORE_REPL_VOLUME_SELF_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    BLOCKSTORE_DRBASED_VOLUME_SELF_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
#undef BLOCKSTORE_SIMPLE_COUNTER

#define BLOCKSTORE_CUMULATIVE_COUNTER(name, ...)                              \
    Cumulative.name.Add(source.Cumulative.name);                              \
//  BLOCKSTORE_CUMULATIVE_COUNTER

    BLOCKSTORE_VOLUME_SELF_COMMON_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
#undef BLOCKSTORE_CUMULATIVE_COUNTER

#define BLOCKSTORE_REQUEST_COUNTER(name, ...)                                 \
    RequestCounters.name.Add(source.RequestCounters.name);                    \
// BLOCKSTORE_REQUEST_COUNTER

    BLOCKSTORE_VOLUME_SELF_COMMON_REQUEST_COUNTERS(BLOCKSTORE_REQUEST_COUNTER)
#undef BLOCKSTORE_REQUEST_COUNTER
}

void TVolumeSelfCounters::Reset()
{
#define BLOCKSTORE_SIMPLE_COUNTER(name, ...)                                  \
    Simple.name.Reset();                                                      \
// BLOCKSTORE_SIMPLE_COUNTER

    BLOCKSTORE_VOLUME_SELF_COMMON_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    BLOCKSTORE_REPL_VOLUME_SELF_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    BLOCKSTORE_DRBASED_VOLUME_SELF_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
#undef BLOCKSTORE_SIMPLE_COUNTER

#define BLOCKSTORE_CUMULATIVE_COUNTER(name, ...)                              \
    Cumulative.name.Reset();                                                  \
// BLOCKSTORE_CUMULATIVE_COUNTER

    BLOCKSTORE_VOLUME_SELF_COMMON_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
#undef BLOCKSTORE_CUMULATIVE_COUNTER

#define BLOCKSTORE_REQUEST_COUNTER(name, ...)                                 \
    RequestCounters.name.Reset();                                             \
// BLOCKSTORE_REQUEST_COUNTER

        BLOCKSTORE_VOLUME_SELF_COMMON_REQUEST_COUNTERS(BLOCKSTORE_REQUEST_COUNTER)
#undef BLOCKSTORE_REQUEST_COUNTER
}

void TVolumeSelfCounters::Register(
    NMonitoring::TDynamicCountersPtr counters,
    bool aggregate)
{
#define BLOCKSTORE_SIMPLE_COUNTER(name, ...)                                  \
    Simple.name.Register(counters, #name);                                    \
// BLOCKSTORE_SIMPLE_COUNTER

    BLOCKSTORE_VOLUME_SELF_COMMON_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    if (Policy == EPublishingPolicy::All || Policy == EPublishingPolicy::Repl) {
        BLOCKSTORE_REPL_VOLUME_SELF_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    }
    if (Policy == EPublishingPolicy::All
            || Policy == EPublishingPolicy::DiskRegistryBased)
    {
        BLOCKSTORE_DRBASED_VOLUME_SELF_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    }
#undef BLOCKSTORE_SIMPLE_COUNTER

#define BLOCKSTORE_CUMULATIVE_COUNTER(name, ...)                              \
    Cumulative.name.Register(counters, #name);                                \
// BLOCKSTORE_CUMULATIVE_COUNTER

    BLOCKSTORE_VOLUME_SELF_COMMON_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
#undef BLOCKSTORE_CUMULATIVE_COUNTER

#define BLOCKSTORE_REQUEST_COUNTER(name, ...)                                 \
    RequestCounters.name.ForceRegister(                                       \
        counters->GetSubgroup("request", #name),                              \
        "ThrottlerDelay",                                                     \
        aggregate);                                                           \
// BLOCKSTORE_REQUEST_COUNTER

    BLOCKSTORE_VOLUME_SELF_COMMON_REQUEST_COUNTERS(BLOCKSTORE_REQUEST_COUNTER)
#undef BLOCKSTORE_REQUEST_COUNTER
}

void TVolumeSelfCounters::Publish(TInstant now)
{
#define BLOCKSTORE_SIMPLE_COUNTER(name, ...)                                  \
    Simple.name.Publish(now);                                                 \
// BLOCKSTORE_SIMPLE_COUNTER

    BLOCKSTORE_VOLUME_SELF_COMMON_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    if (Policy == EPublishingPolicy::All || Policy == EPublishingPolicy::Repl) {
        BLOCKSTORE_REPL_VOLUME_SELF_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    }
    if (Policy == EPublishingPolicy::All
            || Policy == EPublishingPolicy::DiskRegistryBased)
    {
        BLOCKSTORE_DRBASED_VOLUME_SELF_SIMPLE_COUNTERS(BLOCKSTORE_SIMPLE_COUNTER)
    }
#undef BLOCKSTORE_SIMPLE_COUNTER

#define BLOCKSTORE_CUMULATIVE_COUNTER(name, ...)                              \
    Cumulative.name.Publish(now);                                             \
// BLOCKSTORE_CUMULATIVE_COUNTER

    BLOCKSTORE_VOLUME_SELF_COMMON_CUMULATIVE_COUNTERS(BLOCKSTORE_CUMULATIVE_COUNTER)
#undef BLOCKSTORE_CUMULATIVE_COUNTER

#define BLOCKSTORE_REQUEST_COUNTER(name, ...)                                 \
    RequestCounters.name.Publish();                                           \
// BLOCKSTORE_REQUEST_COUNTER

    BLOCKSTORE_VOLUME_SELF_COMMON_REQUEST_COUNTERS(BLOCKSTORE_REQUEST_COUNTER)
#undef BLOCKSTORE_REQUEST_COUNTER

    Reset();
}

////////////////////////////////////////////////////////////////////////////////

TVolumeSelfCountersPtr CreateVolumeSelfCounters(EPublishingPolicy policy)
{
    return std::make_unique<TVolumeSelfCounters>(policy);
}

TPartitionDiskCountersPtr CreatePartitionDiskCounters(EPublishingPolicy policy)
{
    return std::make_unique<TPartitionDiskCounters>(policy);
}

}   // namespace NCloud::NBlockStore::NStorage
