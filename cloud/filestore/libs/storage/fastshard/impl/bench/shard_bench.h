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
 * Each scenario runs with 1 and 8 requests in flight. The factory is
 * invoked once per scenario run and may bring up whatever runtime the
 * implementation needs.
 *
 * @param name - Prefix for the benchmark names.
 * @param factory - Builds the shard under benchmark.
 */
void RegisterShardBenchmarks(TString name, TShardFactory factory);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
