#pragma once

#include <util/generic/ptr.h>
#include <util/system/defaults.h>

#include <atomic>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

/*
 * A set of atomic counters that is supposed to contain the info about the
 * overall state of the node and is supposed to be shared among the long-running
 * actors so that they would have the knowledge about the system being
 * overloaded or not.
 *
 * Actor code is not supposed to try to find counters via MetricsRegistry or any
 * other global counter tree because the paths in those trees aren't guaranteed
 * to stay unchanged and if they change the compiler won't be able to help
 * notice that. If some global counter needs to be shared across multiple
 * actors, it should be put here.
 */
struct TSystemCounters: TAtomicRefCount<TSystemCounters>
{
    std::atomic<ui64> CpuLack; // measured in percent
};

}   // namespace NCloud::NFileStore::NStorage
