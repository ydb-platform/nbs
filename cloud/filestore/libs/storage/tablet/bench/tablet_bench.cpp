#include <cloud/filestore/libs/storage/tablet/tablet.h>

#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/benchmark/bench.h>

#include <util/generic/singleton.h>
#include <util/generic/vector.h>

#include <memory>

using namespace NCloud;
using namespace NCloud::NFileStore;
using namespace NCloud::NFileStore::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 BlockSize = 4096;
constexpr ui64 FileSize = 1_MB;
constexpr ui64 MixedBlocksOffloadedRangesCapacity = 1000000;

struct TTabletSetup
{
    TTestEnv Env;
    std::unique_ptr<TIndexTabletClient> TabletClient;
    ui64 Handle = 0;

    TTabletSetup()
        : Env(TTestEnvConfig{
            // Turn off logging in order to reduce performance overhead
            .LogPriority_NFS = NActors::NLog::PRI_ALERT,
            .LogPriority_KiKiMR = NActors::NLog::PRI_ALERT,
            .LogPriority_Others = NActors::NLog::PRI_ALERT})
    {
        NCloud::NFileStore::NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetMixedBlocksOffloadedRangesCapacity(
            MixedBlocksOffloadedRangesCapacity);

        Env.UpdateStorageConfig(std::move(storageConfig));

        Env.CreateSubDomain("nfs");
        Env.GetRuntime().SetDispatchedEventsLimit(Max<ui64>());

        ui32 nodeIdx = Env.CreateNode("nfs");
        ui64 tabletId = Env.BootIndexTablet(nodeIdx);

        TabletClient = std::make_unique<TIndexTabletClient>(
            Env.GetRuntime(),
            nodeIdx,
            tabletId,
            TFileSystemConfig{.BlockSize = BlockSize});
        TabletClient->InitSession("client", "session");

        auto nodeId = CreateNode(
            *TabletClient,
            TCreateNodeArgs::File(RootNodeId, "test"));

        Handle = CreateHandle(*TabletClient, nodeId);
        TabletClient->WriteData(Handle, 0, FileSize, '1');
    }

    void DescribeData(ui64 offset, ui64 length)
    {
        auto response = TabletClient->DescribeData(Handle, offset, length);
        Y_ABORT_UNLESS(FileSize == response->Record.GetFileSize());
    }
};

TTabletSetup* GetOrCreateTablet()
{
    constexpr ui64 Priority = Max<ui64>();
    return SingletonWithPriority<TTabletSetup, Priority>();
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
