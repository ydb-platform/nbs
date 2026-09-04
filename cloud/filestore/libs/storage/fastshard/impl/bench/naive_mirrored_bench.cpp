#include "delay_policy.h"
#include "null_storage_group.h"
#include "shard_bench.h"

#include <cloud/filestore/libs/storage/fastshard/impl/naive_mirrored/shard.h>
#include <cloud/filestore/private/api/unsafe_protos/unsafe.pb.h>

#include <silk/fibers/fiber.h>
#include <silk/util/init.h>

#include <util/generic/size_literals.h>

#include <cstdlib>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 ShardNo = 1U;
constexpr ui64 NodesPerGroup = 256U;
constexpr ui64 GroupCapacity = 256_MB;

const TDuration StorageDelayMean = TDuration::MicroSeconds(100);
const TDuration StorageDelayStdDev = TDuration::MicroSeconds(100);

////////////////////////////////////////////////////////////////////////////////
// The google benchmark module owns main(), so the silk runtime is
// brought up lazily on first use and torn down via atexit: a scheduler
// thread still running during static destruction segfaults the process.

void EnsureSilk()
{
    static const bool initialized = [] {
        silk::initialize();
        silk::FiberScheduler::initialize();
        std::atexit([] {
            silk::FiberScheduler::destroy();
            silk::destroy();
        });
        return true;
    }();
    Y_UNUSED(initialized);
}

////////////////////////////////////////////////////////////////////////////////
// Naive mirrored shard on top of a null storage group whose responses
// follow a lognormal latency distribution.

IFileSystemShardPtr MakeNaiveMirroredShard()
{
    EnsureSilk();

    NProtoPrivate::TPersistentFastShardConfig config;
    config.SetNodesPerGroup(NodesPerGroup);
    config.SetExpectedGroupCapacity(GroupCapacity);

    return CreateNaiveMirroredFileSystemShard(
        "bench-fs",
        ShardNo,
        CreateNullStorageGroupFactory(CreateLognormalDelayPolicy(
            StorageDelayMean,
            StorageDelayStdDev)),
        config);
}

[[maybe_unused]] const bool registered = [] {
    RegisterShardBenchmarks("NaiveMirroredShard", MakeNaiveMirroredShard);
    return true;
}();

}   // namespace

}   // namespace NCloud::NFileStore::NStorage::NFastShard
