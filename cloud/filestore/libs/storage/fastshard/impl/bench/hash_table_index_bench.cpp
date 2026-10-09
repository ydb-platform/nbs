#include "null_storage_group.h"
#include "shard_bench.h"

#include <cloud/fastshard/bootstrap/core.h>
#include <cloud/fastshard/testlib/delay_policy.h>

#include <cloud/filestore/libs/storage/fastshard/impl/hash_table_index/shard.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <util/generic/size_literals.h>

#include <cstdlib>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 ShardNo = 1U;
constexpr ui64 NodesPerGroup = 256U;
constexpr ui64 GroupCapacity = 256_MB;

////////////////////////////////////////////////////////////////////////////////
// The google benchmark module owns main(), so the silk runtime is
// brought up lazily on first use and torn down via atexit: a scheduler
// thread still running during static destruction segfaults the process.

void EnsureSilk()
{
    static const bool initialized = [] {
        NCloud::NFastShard::Init();
        std::atexit(NCloud::NFastShard::Destroy);
        return true;
    }();
    Y_UNUSED(initialized);
}

////////////////////////////////////////////////////////////////////////////////
// Hash table index shard on top of a null storage group whose responses
// follow a lognormal latency distribution.

IFileSystemShardPtr MakeHashTableIndexShard()
{
    EnsureSilk();

    NProtoPrivate::TPersistentFastShardConfig config;
    config.SetNodesPerGroup(NodesPerGroup);
    config.SetExpectedGroupCapacity(GroupCapacity);
    config.SetPageSize(4_KB);

    return CreateHashTableIndexFileSystemShard(
        "bench-fs",
        ShardNo,
        1 /* generation */,
        CreateNullStorageGroupFactory(
            NCloud::NFastShard::CreateLognormalDelayPolicy(
                NCloud::NFastShard::DefaultStorageDelayMean,
                NCloud::NFastShard::DefaultStorageDelayStdDev)),
        config);
}

[[maybe_unused]] const bool registered = [] {
    RegisterShardBenchmarks("HashTableIndexShard", MakeHashTableIndexShard);
    return true;
}();

}   // namespace

}   // namespace NCloud::NFileStore::NStorage::NFastShard
