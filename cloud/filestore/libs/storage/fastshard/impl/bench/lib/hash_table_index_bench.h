#pragma once

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * Registers the shard benchmarks (see RegisterShardBenchmarks) for the hash
 * table index shard on top of a null storage group with lognormal latencies.
 * The silk runtime is brought up on first use. A process can run only one
 * silk runtime, so a binary has to register the benchmarks only once.
 *
 * @param name - Prefix for the benchmark names.
 * @param maxCpuCount - Max number of the CPUs silk runs on: one CPU per
 *  physical core (the first one of the hyper-threaded siblings) out of the
 *  process affinity mask. Zero means all the CPUs of the affinity mask,
 *  hyper-threaded siblings included.
 */
void RegisterHashTableIndexShardBenchmarks(TString name, ui32 maxCpuCount);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
