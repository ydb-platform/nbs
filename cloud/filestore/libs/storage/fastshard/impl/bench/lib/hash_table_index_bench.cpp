#include "hash_table_index_bench.h"

#include "null_storage_group.h"
#include "shard_bench.h"

#include <cloud/fastshard/libs/bootstrap/core.h>
#include <cloud/fastshard/libs/sn/testlib/delay_policy.h>

#include <cloud/filestore/libs/storage/fastshard/impl/hash_table_index/shard.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/size_literals.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/strip.h>
#include <util/system/fs.h>
#include <util/system/yassert.h>

#include <sched.h>

#include <cerrno>
#include <cstdlib>
#include <mutex>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 ShardNo = 1U;
constexpr ui64 NodesPerGroup = 1024U;
constexpr ui64 GroupCapacity = 1_GB;

////////////////////////////////////////////////////////////////////////////////

cpu_set_t GetAffinityMask()
{
    cpu_set_t mask;
    CPU_ZERO(&mask);
    const int r = ::sched_getaffinity(0, sizeof(mask), &mask);
    Y_ABORT_UNLESS(!r, "sched_getaffinity failed: %d", errno);
    return mask;
}

//
// The CPUs of the same physical core share the core_cpus_list. If the
// topology is not available, every CPU is treated as a separate core.
//

TString GetCoreKey(ui32 cpu)
{
    const TString prefix =
        "/sys/devices/system/cpu/cpu" + ToString(cpu) + "/topology/";
    for (const auto* name: {"core_cpus_list", "thread_siblings_list"}) {
        const TString path = prefix + name;
        if (NFs::Exists(path)) {
            return Strip(TFileInput(path).ReadAll());
        }
    }

    return ToString(cpu);
}

//
// Takes the CPUs of the affinity mask in ascending order, one CPU per
// physical core (the first one of the hyper-threaded siblings), until
// maxCpuCount CPUs are taken.
//

cpu_set_t SelectCpus(ui32 maxCpuCount)
{
    const cpu_set_t affinity = GetAffinityMask();

    cpu_set_t selected;
    CPU_ZERO(&selected);

    THashSet<TString> cores;
    ui32 count = 0;
    for (ui32 cpu = 0; cpu < CPU_SETSIZE && count < maxCpuCount; ++cpu) {
        if (!CPU_ISSET(cpu, &affinity)) {
            continue;
        }

        if (cores.insert(GetCoreKey(cpu)).second) {
            CPU_SET(cpu, &selected);
            ++count;
        }
    }

    Y_ABORT_UNLESS(count, "no cpus selected");
    return selected;
}

////////////////////////////////////////////////////////////////////////////////
// The google benchmark module owns main(), so the silk runtime is
// brought up lazily on first use and torn down via atexit: a scheduler
// thread still running during static destruction segfaults the process.

void EnsureSilk(const TMaybe<cpu_set_t>& cpuMask)
{
    static std::once_flag initialized;
    std::call_once(
        initialized,
        [&]
        {
            if (cpuMask) {
                NCloud::NFastShard::Init(*cpuMask);
            } else {
                NCloud::NFastShard::Init();
            }
            std::atexit(NCloud::NFastShard::Destroy);
        });
}

////////////////////////////////////////////////////////////////////////////////
// Hash table index shard on top of a null storage group whose responses
// follow a lognormal latency distribution.

IFileSystemShardPtr MakeHashTableIndexShard(const TMaybe<cpu_set_t>& cpuMask)
{
    EnsureSilk(cpuMask);

    NProtoPrivate::TPersistentFastShardConfig config;
    config.SetNodesPerGroup(NodesPerGroup);
    config.SetExpectedGroupCapacity(GroupCapacity);
    config.SetPageSize(4_KB);

    auto delayPolicy = NCloud::NFastShard::CreateLognormalDelayPolicy(
        NCloud::NFastShard::DefaultStorageDelayMean,
        NCloud::NFastShard::DefaultStorageDelayStdDev);

    return CreateHashTableIndexFileSystemShard(
        "bench-fs",
        ShardNo,
        1 /* generation */,
        CreateNullStorageGroup(std::move(delayPolicy), config.GetPageSize()),
        config);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void RegisterHashTableIndexShardBenchmarks(TString name, ui32 maxCpuCount)
{
    TMaybe<cpu_set_t> cpuMask;
    if (maxCpuCount) {
        cpuMask = SelectCpus(maxCpuCount);
    }

    const cpu_set_t effectiveMask = cpuMask ? *cpuMask : GetAffinityMask();
    const ui32 cpuCount = CPU_COUNT(&effectiveMask);

    RegisterShardBenchmarks(
        std::move(name),
        [cpuMask] { return MakeHashTableIndexShard(cpuMask); },
        cpuCount);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
