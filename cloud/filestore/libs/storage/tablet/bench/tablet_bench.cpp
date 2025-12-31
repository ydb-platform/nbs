#include <cloud/filestore/libs/storage/tablet/tablet.h>

#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/benchmark/bench.h>

#include <util/generic/vector.h>

#include <atomic>
#include <memory>

using namespace NCloud;
using namespace NCloud::NFileStore;
using namespace NCloud::NFileStore::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTabletSetup
{
    TTestEnv Env;
    std::unique_ptr<TIndexTabletClient> TabletClient;
    ui64 Handle = 0;

    TTabletSetup()
        : Env(TTestEnvConfig{
            .LogPriority_NFS = NActors::NLog::PRI_ALERT,
            .LogPriority_KiKiMR = NActors::NLog::PRI_ALERT,
            .LogPriority_Others = NActors::NLog::PRI_ALERT})
    {
        NCloud::NFileStore::NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetMixedBlocksOffloadedRangesCapacity(1e6);

        Env.UpdateStorageConfig(std::move(storageConfig));

        Env.CreateSubDomain("nfs");
        Env.GetRuntime().SetDispatchedEventsLimit(1e9);

        ui32 nodeIdx = Env.CreateNode("nfs");
        ui64 tabletId = Env.BootIndexTablet(nodeIdx);

        TabletClient = std::make_unique<TIndexTabletClient>(
            Env.GetRuntime(),
            nodeIdx,
            tabletId,
            TFileSystemConfig{.BlockSize = 4096});
        TabletClient->InitSession("client", "session");

        auto nodeId = CreateNode(
            *TabletClient,
            TCreateNodeArgs::File(RootNodeId, "test"));

        Handle = CreateHandle(*TabletClient, nodeId);
        TabletClient->WriteData(Handle, 0, 1_MB, '1');
    }

    void DescribeData(ui64 offset, ui64 length)
    {
        auto response = TabletClient->DescribeData(Handle, offset, length);
        Y_ABORT_UNLESS(1_MB == response->Record.GetFileSize());
    }
};

// Using a non-owning static variable because the test Actor System crashes
// during initialization.
// A singleton also doesn’t work because it causes
// other issues during deinitialization.
// A memory leak is the price we pay to prevent the program from crashing.
// This is acceptable because benchmarks are not run under sanitizers
std::atomic<TTabletSetup*> Tablet = nullptr;

TTabletSetup* GetOrCreateTablet()
{
    if (Tablet.load()) {
        return Tablet.load();
    }

    auto tmp = std::make_unique<TTabletSetup>();
    TTabletSetup* expected = nullptr;
    if (!Tablet.compare_exchange_strong(expected, tmp.get())) {
        return Tablet.load();
    }

    return tmp.release();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_CPU_BENCHMARK(TTablet_DescribeData_1MiBRequestSize, iface)
{
    auto* tablet = GetOrCreateTablet();

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        tablet->DescribeData(0, 1_MB);
    }
}
