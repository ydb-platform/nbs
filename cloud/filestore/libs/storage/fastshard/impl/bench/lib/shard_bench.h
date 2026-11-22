#pragma once

#include <cloud/filestore/libs/storage/fastshard/iface/public.h>

#include <util/generic/string.h>

#include <functional>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

using TShardFactory = std::function<IFileSystemShardPtr()>;

/**
 * Registers the benchmark scenarios for one IFileSystemShard
 * implementation:
 * - create/unlink node pairs;
 * - 4K writes and reads over a set of precreated nodes.
 * Each scenario runs with 1, 8, 32 and 256 requests kept in flight all the
 * time. The factory is invoked once per scenario run and may bring up
 * whatever runtime the implementation needs.
 *
 * @param name - Prefix for the benchmark names.
 * @param factory - Builds the shard under benchmark.
 * @param cpuCount - Number of the CPUs the implementation runs on, reported
 *  as the cpus counter.
 */
void RegisterShardBenchmarks(
    TString name,
    TShardFactory factory,
    ui32 cpuCount);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
