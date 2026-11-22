#include <cloud/filestore/libs/storage/fastshard/impl/bench/lib/hash_table_index_bench.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

[[maybe_unused]] const bool registered = [] {
    RegisterHashTableIndexShardBenchmarks(
        "HashTableIndexShardCpus32",
        32 /* maxCpuCount */);
    return true;
}();

}   // namespace

}   // namespace NCloud::NFileStore::NStorage::NFastShard
