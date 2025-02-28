#include "tablet.h"

#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TFileSystemConfig MakeThrottlerConfig(
    bool throttlingEnabled,
    ui32 maxReadIops,
    ui32 maxWriteIops,
    ui64 maxReadBandwidth,
    ui64 maxWriteBandwidth,
    ui32 boostTime,
    ui32 boostRefillTime,
    ui32 boostPercentage,
    ui64 maxPostponedWeight,
    ui32 maxWriteCostMultiplier,
    ui32 maxPostponedTime,
    ui32 maxPostponedCount,
    ui32 burstPercentage,
    ui64 defaultPostponedRequestWeight)
{
    TFileSystemConfig config;
    config.PerformanceProfile.ThrottlingEnabled = throttlingEnabled;
    config.PerformanceProfile.MaxReadIops = maxReadIops;
    config.PerformanceProfile.MaxWriteIops = maxWriteIops;
    config.PerformanceProfile.MaxReadBandwidth = maxReadBandwidth;
    config.PerformanceProfile.MaxWriteBandwidth = maxWriteBandwidth;
    config.PerformanceProfile.BoostTime = boostTime;
    config.PerformanceProfile.BoostRefillTime = boostRefillTime;
    config.PerformanceProfile.BoostPercentage = boostPercentage;
    config.PerformanceProfile.MaxPostponedWeight = maxPostponedWeight;
    config.PerformanceProfile.MaxWriteCostMultiplier = maxWriteCostMultiplier;
    config.PerformanceProfile.MaxPostponedTime = maxPostponedTime;
    config.PerformanceProfile.MaxPostponedCount = maxPostponedCount;
    config.PerformanceProfile.BurstPercentage = burstPercentage;
    config.PerformanceProfile.DefaultPostponedRequestWeight =
        defaultPostponedRequestWeight;

    return config;
}

////////////////////////////////////////////////////////////////////////////////

NProto::TDiagnosticsConfig MakeDiagConfig()
{
    NProto::TDiagnosticsConfig config;
    NProto::TFileSystemPerformanceProfile pp;
    pp.MutableRead()->SetRPS(1000);
    pp.MutableRead()->SetThroughput(100_MB);
    pp.MutableWrite()->SetRPS(2000);
    pp.MutableWrite()->SetThroughput(200_MB);
    pp.MutableListNodes()->SetRPS(10);
    pp.MutableGetNodeAttr()->SetRPS(5000);
    pp.MutableCreateHandle()->SetRPS(100);
    pp.MutableDestroyHandle()->SetRPS(40);
    pp.MutableCreateNode()->SetRPS(50);
    pp.MutableRenameNode()->SetRPS(70);
    pp.MutableUnlinkNode()->SetRPS(80);
    pp.MutableStatFileStore()->SetRPS(15);

    *config.MutableSSDFileSystemPerformanceProfile() = pp;
    *config.MutableHDDFileSystemPerformanceProfile() = pp;
    return config;
}

////////////////////////////////////////////////////////////////////////////////

struct TEnv
    : public NUnitTest::TBaseFixture
{
    TTestEnv Env;
    std::unique_ptr<TIndexTabletClient> Tablet;

    TTestRegistryVisitor Visitor;

    TEnv()
        : Env(
            TTestEnvConfig{},
            NProto::TStorageConfig{},
            NKikimr::NFake::TCaches{},
            CreateProfileLogStub(),
            MakeDiagConfig())
    {}

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Env.CreateSubDomain("nfs");

        const ui32 nodeIdx = Env.CreateNode("nfs");
        const ui64 tabletId = Env.BootIndexTablet(nodeIdx);

        Tablet = std::make_unique<TIndexTabletClient>(
            Env.GetRuntime(),
            nodeIdx,
            tabletId);
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}

};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Counters)
{
    Y_UNIT_TEST_F(ShouldRegisterCounters, TEnv)
    {
        auto registry = Env.GetRegistry();

        registry->Visit(TInstant::Zero(), Visitor);
        // clang-format off
        Visitor.ValidateExpectedCounters({
            {{
                {"component", "storage_fs"},
                {"host", "cluster"},
                {"filesystem", "test"},
                {"sensor", "FreshBytesCount"}}, 0},
            {{
                {"component", "storage_fs"},
                {"host", "cluster"},
                {"filesystem", "test"},
                {"sensor", "GarbageQueueSize"}}, 0},
            {{
                {"component", "storage_fs"},
                {"host", "cluster"},
                {"filesystem", "test"},
                {"sensor", "MixedBytesCount"}}, 0},
            {{
                {"component", "storage_fs"},
                {"host", "cluster"},
                {"filesystem", "test"},
                {"sensor", "MixedBlobsCount"}}, 0},
            {{
                {"component", "storage_fs"},
                {"host", "cluster"},
                {"filesystem", "test"},
                {"sensor", "GarbageQueueSize"}}, 0},
            {{
                {"component", "storage_fs"},
                {"host", "cluster"},
                {"filesystem", "test"},
                {"sensor", "GarbageBytesCount"}}, 0},
            {{
                {"component", "storage_fs"},
                {"host", "cluster"},
                {"filesystem", "test"},
                {"sensor", "FreshBlocksCount"}}, 0},
            {{
                {"component", "storage_fs"},
                {"host", "cluster"},
                {"filesystem", "test"},
                {"sensor", "PostponedRequests"}}, 0},
            {{
                {"component", "storage_fs"},
                {"host", "cluster"},
                {"filesystem", "test"},
                {"sensor", "RejectedRequests"}}, 0},
            {{
                {"component", "storage_fs"},
                {"host", "cluster"},
                {"filesystem", "test"},
                {"sensor", "UsedSessionsCount"}}, 0},
            {{
                {"component", "storage_fs"},
                {"host", "cluster"},
                {"filesystem", "test"},
                {"sensor", "UsedBytesCount"}}, 0},
            {{
                {"component", "storage_fs"},
                {"host", "cluster"},
                {"filesystem", "test"},
                {"sensor", "UsedQuota"}}, 0},
            {{
                {"component", "storage"},
                {"type", "hdd"},
                {"sensor", "FreshBytesCount"}}, 0},
            {{
                {"component", "storage"},
                {"type", "hdd"},
                {"sensor", "GarbageQueueSize"}}, 0},
            {{
                {"component", "storage"},
                {"type", "hdd"},
                {"sensor", "MixedBytesCount"}}, 0},
            {{
                {"component", "storage"},
                {"type", "hdd"},
                {"sensor", "UsedSessionsCount"}}, 0},
            {{
                {"component", "storage"},
                {"type", "hdd"},
                {"sensor", "UsedBytesCount"}}, 0},
            {{
                {"component", "storage"},
                {"type", "hdd"},
                {"sensor", "MixedBlobsCount"}}, 0},
            {{
                {"component", "storage"},
                {"type", "hdd"},
                {"sensor", "GarbageQueueSize"}}, 0},
            {{
                {"component", "storage"},
                {"type", "hdd"},
                {"sensor", "GarbageBytesCount"}}, 0},
            {{
                {"component", "storage"},
                {"type", "hdd"},
                {"sensor", "FreshBlocksCount"}}, 0}
        });
        // clang-format on
    }

    Y_UNIT_TEST_F(ShouldCorrectlyWriteThrottlerMaxParams, TEnv)
    {
        auto registry = Env.GetRegistry();

        Tablet->AdvanceTime(TDuration::Seconds(15));
        Env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));

        registry->Visit(TInstant::Zero(), Visitor);
        Visitor.ValidateExpectedCounters({
            {{{"sensor", "MaxWriteBandwidth"}}, Max<i64>()},
            {{{"sensor", "MaxReadIops"}}, 4_GB - 1},
            {{{"sensor", "MaxWriteIops"}}, 4_GB - 1},
            {{{"sensor", "MaxReadBandwidth"}}, Max<i64>()}
        });
        auto config = MakeThrottlerConfig(
            true,                                    // throttlingEnabled
            100,                                     // maxReadIops
            200,                                     // maxWriteIops
            4_KB,                                    // maxReadBandwidth
            8_KB,                                    // maxWriteBandwidth
            TDuration::Minutes(30).MilliSeconds(),   // boostTime
            TDuration::Hours(12).MilliSeconds(),     // boostRefillTime
            10,                                      // boostPercentage
            32_KB,                                   // maxPostponedWeight
            10,                                      // maxWriteCostMultiplier
            TDuration::Seconds(25).MilliSeconds(),   // maxPostponedTime
            64,                                      // maxPostponedCount
            100,                                     // burstPercentage
            1_KB                                     // defaultPostponedRequestWeight
        );
        Tablet->UpdateConfig(config);

        Tablet->AdvanceTime(TDuration::Seconds(15));
        Env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));

        registry->Visit(TInstant::Zero(), Visitor);
        Visitor.ValidateExpectedCounters({
            {{{"sensor", "MaxWriteBandwidth"}}, 8_KB},
            {{{"sensor", "MaxReadIops"}}, 100},
            {{{"sensor", "MaxWriteIops"}}, 200},
            {{{"sensor", "MaxReadBandwidth"}}, 4_KB}
        });

        config.PerformanceProfile.MaxWriteBandwidth = 3_GB;
        config.PerformanceProfile.MaxReadBandwidth = 5_GB;
        Tablet->UpdateConfig(config);
        Env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));
        registry->Visit(TInstant::Zero(), Visitor);
        Visitor.ValidateExpectedCounters({
            {{{"sensor", "MaxWriteBandwidth"}}, 3_GB},
            {{{"sensor", "MaxReadBandwidth"}}, 5_GB},
        });
    }

    Y_UNIT_TEST_F(ShouldIncrementAndDecrementSessionCount, TEnv)
    {
        auto registry = Env.GetRegistry();

        registry->Visit(TInstant::Zero(), Visitor);
        Visitor.ValidateExpectedCounters({
            {{{"sensor", "UsedSessionsCount"}, {"filesystem", "test"}}, 0},
            {{{"sensor", "StatefulSessionsCount"}, {"filesystem", "test"}}, 0},
            {{{"sensor", "StatelessSessionsCount"}, {"filesystem", "test"}}, 0},
            {{{"sensor", "SessionTimeouts"}, {"filesystem", "test"}}, 0},
        });

        Tablet->InitSession("client", "session");

        Tablet->AdvanceTime(TDuration::Seconds(15));
        Env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));

        registry->Visit(TInstant::Zero(), Visitor);
        Visitor.ValidateExpectedCounters({
            {{{"sensor", "UsedSessionsCount"}, {"filesystem", "test"}}, 1},
            {{{"sensor", "StatefulSessionsCount"}, {"filesystem", "test"}}, 0},
            {{{"sensor", "StatelessSessionsCount"}, {"filesystem", "test"}}, 1},
            {{{"sensor", "SessionTimeouts"}, {"filesystem", "test"}}, 0},
        });

        Tablet->ResetSession("client", "session", 0, "state");

        Tablet->AdvanceTime(TDuration::Seconds(15));
        Env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));

        registry->Visit(TInstant::Zero(), Visitor);
        Visitor.ValidateExpectedCounters({
            {{{"sensor", "UsedSessionsCount"}, {"filesystem", "test"}}, 1},
            {{{"sensor", "StatefulSessionsCount"}, {"filesystem", "test"}}, 1},
            {{{"sensor", "StatelessSessionsCount"}, {"filesystem", "test"}}, 0},
            {{{"sensor", "SessionTimeouts"}, {"filesystem", "test"}}, 0},
        });

        Tablet->RebootTablet();
        Tablet->AdvanceTime(TDuration::Hours(1));
        Tablet->CleanupSessions();

        Tablet->AdvanceTime(TDuration::Seconds(15));
        Env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));

        registry->Visit(TInstant::Zero(), Visitor);
        Visitor.ValidateExpectedCounters({
            {{{"sensor", "UsedSessionsCount"}, {"filesystem", "test"}}, 0},
            {{{"sensor", "StatefulSessionsCount"}, {"filesystem", "test"}}, 0},
            {{{"sensor", "StatelessSessionsCount"}, {"filesystem", "test"}}, 0},
            {{{"sensor", "SessionTimeouts"}, {"filesystem", "test"}}, 1},
        });
    }

    Y_UNIT_TEST_F(ShouldCorrectlyUpdateValuesOnThrottling, TEnv)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetThrottlingEnabled(true);

        TTestEnv env({}, storageConfig);
        auto registry = env.GetRegistry();

        env.CreateSubDomain("nfs");
        const auto nodeIdx = env.CreateNode("nfs");
        const auto tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");
        const auto nodeId =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        const auto handle = CreateHandle(tablet, nodeId);

        const auto config = MakeThrottlerConfig(
            true,                                    // throttlingEnabled
            2,                                       // maxReadIops
            2,                                       // maxWriteIops
            8_KB,                                    // maxReadBandwidth
            8_KB,                                    // maxWriteBandwidth
            TDuration::Minutes(30).MilliSeconds(),   // boostTime
            TDuration::Hours(12).MilliSeconds(),     // boostRefillTime
            10,                                      // boostPercentage
            32_KB,                                   // maxPostponedWeight
            10,                                      // maxWriteCostMultiplier
            TDuration::Seconds(25).MilliSeconds(),   // maxPostponedTime
            64,                                      // maxPostponedCount
            100,                                     // burstPercentage
            1_KB                                     // defaultPostponedRequestWeight
        );
        tablet.UpdateConfig(config);

        for (size_t i = 0; i < 10; ++i) {
            tablet.AdvanceTime(TDuration::Seconds(1));
            tablet.SendReadDataRequest(handle, 4_KB * i, 4_KB);
            tablet.AssertReadDataQuickResponse(S_OK);

            tablet.AdvanceTime(TDuration::Seconds(1));
            tablet.SendWriteDataRequest(
                handle,
                4_KB * (i + 1),
                4_KB,
                static_cast<char>('a' + i));
            tablet.AssertWriteDataQuickResponse(S_OK);
        }

        // 1. Testing that excess requests are postponed.
        for (size_t i = 0; i < 20; ++i) {
            tablet.SendReadDataRequest(handle, 4_KB * i, 4_KB);
            tablet.AssertReadDataNoResponse();
        }

        registry->Visit(TInstant::Zero(), Visitor);
        Visitor.ValidateExpectedCounters({
            {{{"sensor", "UsedQuota"}}, 100},
            {{{"sensor", "MaxUsedQuota"}}, 100},
            {{{"sensor", "MaxWriteBandwidth"}}, 8_KB},
            {{{"sensor", "MaxReadBandwidth"}}, 8_KB},
            {{{"sensor", "MaxWriteIops"}}, 2},
            {{{"sensor", "MaxReadIops"}}, 2},
            {{{"sensor", "PostponedRequests"}}, 20},
        });

        // Now we have 20_KB in PostponeQueue.

        for (size_t i = 0; i < 3; ++i) {
            tablet.SendWriteDataRequest(handle, 0, 4_KB, 'z');
            tablet.AssertWriteDataNoResponse();
        }

        registry->Visit(TInstant::Zero(), Visitor);
        Visitor.ValidateExpectedCounters({
            {{{"sensor", "PostponedRequests"}}, 23},
        });

        // Now we have 32_KB in PostponedQueue. Equal to MaxPostponedWeight.

        // 2. Testing that we start rejecting requests after
        // our postponed limit saturates.
        tablet.SendWriteDataRequest(handle, 0, 1, 'y');   // Event 1 byte request must be rejected.
        tablet.AssertWriteDataQuickResponse(E_FS_THROTTLED);
        tablet.SendReadDataRequest(handle, 0, 1_KB);
        tablet.AssertReadDataQuickResponse(E_FS_THROTTLED);

        registry->Visit(TInstant::Zero(), Visitor);
        Visitor.ValidateExpectedCounters({
            {{{"sensor", "RejectedRequests"}}, 2},
        });
    }

    Y_UNIT_TEST_F(ShouldCalculateReadWriteMetrics, TEnv)
    {
        auto registry = Env.GetRegistry();

        Tablet->InitSession("client", "session");

        auto id =
            CreateNode(*Tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(*Tablet, id);

        const auto sz = 256_KB;

        Tablet->WriteData(handle, 0, sz, 'a');

        {
            auto response = Tablet->GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlocksCount(), sz / 4_KB);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetMixedBlobsCount(), 1);
        }

        {
            auto response = Tablet->ReadData(handle, 0, sz);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, sz, 'a'));
        }

        {
            auto response = Tablet->DescribeData(handle, 0, sz);
            const auto& blobs = response->Record.GetBlobPieces();
            UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
        }

        {
            auto response = Tablet->GenerateBlobIds(id, handle, 0, sz);
            TVector<NKikimr::TLogoBlobID> blobIds;
            for (const auto& blobId: response->Record.GetBlobs()) {
                auto blob = NKikimr::LogoBlobIDFromLogoBlobID(blobId.GetBlobId());
                blobIds.push_back(blob);
            }
            Tablet->AddData(id, handle, 0, sz, blobIds, response->Record.GetCommitId());
        }

        registry->Visit(TInstant::Zero(), Visitor);
        Visitor.ValidateExpectedHistogram({
            {{
                {"histogram", "Time"},
                {"filesystem", "test"},
                {"request", "WriteBlob"}}, 0},
            {{
                {"histogram", "Time"},
                {"filesystem", "test"},
                {"request", "ReadBlob"}}, 0},
            {{
                {"histogram", "Time"},
                {"filesystem", "test"},
                {"request", "WriteData"}}, 0},
            {{
                {"histogram", "Time"},
                {"filesystem", "test"},
                {"request", "ReadData"}}, 0},
            {{
                {"histogram", "Time"},
                {"filesystem", "test"},
                {"request", "DescribeData"}}, 0},
            {{
                {"histogram", "Time"},
                {"filesystem", "test"},
                {"request", "GenerateBlobIds"}}, 0},
            {{
                {"histogram", "Time"},
                {"filesystem", "test"},
                {"request", "AddData"}}, 0},
        }, false);
        Visitor.ValidateExpectedHistogram({
            {{
                {"histogram", "Time"},
                {"filesystem", "test"},
                {"request", "PatchBlob"}}, 0},
        }, true);
        Visitor.ValidateExpectedCounters({
            {{
                {"sensor", "WriteBlob.Count"},
                {"filesystem", "test"}}, 1},
            {{
                {"sensor", "ReadBlob.Count"},
                {"filesystem", "test"}}, 1},
            {{
                {"sensor", "PatchBlob.Count"},
                {"filesystem", "test"}}, 0},
            {{
                {"sensor", "WriteData.Count"},
                {"filesystem", "test"}}, 1},
            {{
                {"sensor", "ReadData.Count"},
                {"filesystem", "test"}}, 1},
            {{
                {"sensor", "DescribeData.Count"},
                {"filesystem", "test"}}, 1},
            {{
                {"sensor", "GenerateBlobIds.Count"},
                {"filesystem", "test"}}, 1},
            {{
                {"sensor", "AddData.Count"},
                {"filesystem", "test"}}, 1},
        });
        Visitor.ValidateExpectedCounters({
            {{
                {"sensor", "WriteBlob.RequestBytes"},
                {"filesystem", "test"}}, sz},
            {{
                {"sensor", "ReadBlob.RequestBytes"},
                {"filesystem", "test"}}, sz},
            {{
                {"sensor", "PatchBlob.RequestBytes"},
                {"filesystem", "test"}}, 0},
            {{
                {"sensor", "WriteData.RequestBytes"},
                {"filesystem", "test"}}, sz},
            {{
                {"sensor", "ReadData.RequestBytes"},
                {"filesystem", "test"}}, sz},
            {{
                {"sensor", "DescribeData.RequestBytes"},
                {"filesystem", "test"}}, sz},
            {{
                {"sensor", "GenerateBlobIds.RequestBytes"},
                {"filesystem", "test"}}, sz},
            {{
                {"sensor", "AddData.RequestBytes"},
                {"filesystem", "test"}}, sz},
            {{
                {"sensor", "CurrentLoad"},
                {"filesystem", "test"}}, 0},
        });

        Tablet->AdvanceTime(TDuration::Seconds(15));
        Env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));
        registry->Visit(TInstant::Zero(), Visitor);
        Visitor.ValidateExpectedCounters({
            {{
                {"sensor", "CurrentLoad"},
                {"filesystem", "test"}}, 7},
            {{
                {"sensor", "Suffer"},
                {"filesystem", "test"}}, 0},
        });

        Tablet->DestroyHandle(handle);
    }

    Y_UNIT_TEST_F(ShouldCalculateLoadAndSuffer, TEnv)
    {
        auto registry = Env.GetRegistry();

        Tablet->InitSession("client", "session");

        auto id =
            CreateNode(*Tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(*Tablet, id);

        const auto sz = 256_KB;

        Tablet->WriteData(handle, 0, sz, 'a');
        Tablet->ListNodes(RootNodeId);
        Tablet->GetNodeAttr(id);
        Tablet->RenameNode(RootNodeId, "test", RootNodeId, "test2");
        Tablet->UnlinkNode(RootNodeId, "test2", false);
        Tablet->GetStorageStats();

        {
            auto response = Tablet->ReadData(handle, 0, sz);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT(CompareBuffer(buffer, sz, 'a'));
        }

        {
            auto response = Tablet->DescribeData(handle, 0, sz);
            const auto& blobs = response->Record.GetBlobPieces();
            UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
        }

        Tablet->DestroyHandle(handle);

        Tablet->AdvanceTime(TDuration::Seconds(15));
        Env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));
        registry->Visit(TInstant::Zero(), Visitor);
        Visitor.ValidateExpectedCounters({
            {{
                {"sensor", "CurrentLoad"},
                {"filesystem", "test"}}, 17},
            {{
                {"sensor", "Suffer"},
                {"filesystem", "test"}}, 0},
        });

        // hard to guarantee something better than the absence of overflows
        const auto latencySensorPredicate = [] (i64 val) {
            return val < 1e9;
        };
        TVector<std::pair<
            TVector<TTestRegistryVisitor::TLabel>,
            std::function<bool(i64)>>> expectedCounters;
        const auto requestNames = {
            "WriteData",
            "ReadData",
            "ListNodes",
            "GetNodeAttr",
            "RenameNode",
            "UnlinkNode",
            "StatFileStore",
            "DescribeData",
            "DestroyHandle",
        };
        for (const auto& requestName: requestNames) {
            expectedCounters.push_back(std::make_pair(
                TVector<TTestRegistryVisitor::TLabel>({
                    {"sensor", Sprintf("%s.TimeSumUs", requestName)},
                    {"filesystem", "test"}
                }),
                latencySensorPredicate
            ));
        }
        Visitor.ValidateExpectedCountersWithPredicate(expectedCounters);
    }

    Y_UNIT_TEST(ShouldCorrectlyWriteCompactionStats)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetThrottlingEnabled(false);

        storageConfig.SetCompactionThreshold(std::numeric_limits<ui32>::max());
        storageConfig.SetCleanupThreshold(std::numeric_limits<ui32>::max());

        TTestEnv env({}, storageConfig);
        auto registry = env.GetRegistry();

        env.CreateSubDomain("nfs");
        const auto nodeIdx = env.CreateNode("nfs");
        const auto tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");
        const auto nodeId =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        const auto handle = CreateHandle(tablet, nodeId);

        const size_t countrRewrites = 7;

        for (size_t i = 0; i < countrRewrites; ++i) {
            tablet.AdvanceTime(TDuration::Seconds(1));
            tablet.SendWriteDataRequest(
                handle,
                0,
                DefaultBlockSize * BlockGroupSize,
                static_cast<char>('a'));
            tablet.AssertWriteDataQuickResponse(S_OK);
        }
        tablet.Flush();
        tablet.FlushBytes();

        tablet.AdvanceTime(TDuration::Seconds(15));
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));

        TTestRegistryVisitor visitor;

        const auto deletions = countrRewrites * BlockGroupSize;
        registry->Visit(TInstant::Zero(), visitor);
        // clang-format off
        visitor.ValidateExpectedCounters({
            {{{"sensor", "MaxBlobsInRange"}}, countrRewrites},
            {{{"sensor", "MaxDeletionsInRange"}}, deletions},
            {{{"sensor", "MaxGarbageBlocksInRange"}}, 0},
        });
        // clang-format on
    }

    Y_UNIT_TEST(ShouldProperlyReportDataMetrics)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetThrottlingEnabled(false);

        TTestEnv env({}, storageConfig);
        auto registry = env.GetRegistry();

        env.CreateSubDomain("nfs");
        const auto nodeIdx = env.CreateNode("nfs");
        const auto tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");
        const auto nodeId =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        const auto handle = CreateHandle(tablet, nodeId);

        tablet.WriteData(handle, 1, DefaultBlockSize, static_cast<char>('a'));
        const auto sz = DefaultBlockSize * BlockGroupSize;
        tablet.WriteData(handle, sz * 10, sz, 'a');

        tablet.AdvanceTime(TDuration::Seconds(15));
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));

        TTestRegistryVisitor visitor;
        // clang-format off
        registry->Visit(TInstant::Zero(), visitor);
        visitor.ValidateExpectedCounters({
            {{{"sensor", "FreshBytesCount"}, {"filesystem", "test"}}, DefaultBlockSize - 1},
            {{{"sensor", "FreshBlocksCount"}, {"filesystem", "test"}}, 1},
            {{{"sensor", "MixedBlobsCount"}, {"filesystem", "test"}}, 1},
            {{{"sensor", "CMMixedBlobsCount"}, {"filesystem", "test"}}, 1},
            {{{"sensor", "DeletionMarkersCount"}, {"filesystem", "test"}}, 64},
            {{{"sensor", "CMDeletionMarkersCount"}, {"filesystem", "test"}}, 64},
            {{{"sensor", "MixedBytesCount"}, {"filesystem", "test"}}, sz},
            {{{"sensor", "CMGarbageBlocksCount"}, {"filesystem", "test"}}, 0},
        });
        // clang-format on

        tablet.FlushBytes();
    }

    Y_UNIT_TEST(ShouldProperlyReportMetricsToHive)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetGarbageCompactionThresholdAverage(10000);
        storageConfig.SetCompactionThresholdAverage(10000);

        storageConfig.SetThrottlingEnabled(false);

        TTestEnv env({}, storageConfig);
        auto registry = env.GetRegistry();

        env.CreateSubDomain("nfs");
        const auto nodeIdx = env.CreateNode("nfs");
        const auto tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");
        const auto nodeId =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        const auto handle = CreateHandle(tablet, nodeId);
        const int blockCount = 1024;
        const auto sz = DefaultBlockSize * blockCount;

        ui64 reportCount = 0;
        i64 network = 0;
        const i64 reportInterval = 15;

        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case NKikimr::TEvLocal::EvTabletMetrics: {
                        const auto* msg = event->template Get<
                            NKikimr::TEvLocal::TEvTabletMetrics>();
                        if (tabletId != msg->TabletId) {
                            break;
                        }
                        ++reportCount;
                        NKikimrTabletBase::TMetrics metrics =
                            msg->ResourceValues;
                        network += metrics.GetNetwork();
                    }
                }
                return false;
            });

        // First metrics submission simply sets the submission time, but does not provide any metrics.
        // So we wait for the initial submission before generating the load.
        env.GetRuntime().AdvanceCurrentTime(TDuration::Seconds(reportInterval));
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(reportInterval));

        tablet.WriteData(handle, 0, sz, 'a');

        env.GetRuntime().AdvanceCurrentTime(TDuration::Seconds(reportInterval));
        {
            NActors::TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvIndexTabletPrivate::EvUpdateCounters);
            env.GetRuntime().DispatchEvents(options);
        }

        NActors::TDispatchOptions options;
        options.FinalEvents.emplace_back(NKikimr::TEvLocal::EvTabletMetrics);
        env.GetRuntime().DispatchEvents(
            NActors::TDispatchOptions{
                .CustomFinalCondition = [&]()
                {
                    return reportCount;
                }
            }, TDuration::Seconds(reportInterval));

        UNIT_ASSERT_DOUBLES_EQUAL(sz, (network * reportInterval), sz / 100);
    }

    Y_UNIT_TEST(ShouldReportCompressionMetrics)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetBlobCompressionRate(1);
        storageConfig.SetWriteBlobThreshold(1);

        TTestEnv env({}, std::move(storageConfig));
        auto registry = env.GetRegistry();

        env.CreateSubDomain("nfs");
        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");
        const auto nodeId =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        const auto handle = CreateHandle(tablet, nodeId);

        tablet.WriteData(handle, 0, 100_KB, 'a');

        TTestRegistryVisitor visitor;
        registry->Visit(TInstant::Zero(), visitor);
        visitor.ValidateExpectedCounters({
            {
                {
                    {"sensor", "UncompressedBytesWritten"},
                    {"filesystem", "test"}
                },
                100_KB // expected
            },
            {
                {
                    {"sensor", "CompressedBytesWritten"},
                    {"filesystem", "test"}
                },
                439 // expected
            },
        });
    }

    Y_UNIT_TEST(ShouldNotReportCompressionMetricsForAllBlobs)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetBlobCompressionRate(2);
        storageConfig.SetWriteBlobThreshold(1);

        TTestEnv env({}, std::move(storageConfig));
        auto registry = env.GetRegistry();

        env.CreateSubDomain("nfs");
        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");
        const auto nodeId =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        const auto handle = CreateHandle(tablet, nodeId);

        for (int i = 0; i < 10; i++)
            tablet.WriteData(handle, 0, 4_KB, 'a');

        TTestRegistryVisitor visitor;
        registry->Visit(TInstant::Zero(), visitor);
        visitor.ValidateExpectedCountersWithPredicate({
            {
                {
                    {"sensor", "UncompressedBytesWritten"},
                    {"filesystem", "test"}
                },
                [](i64 val) { return val > 0 && val < 40960; } // expected
            },
            {
                {
                    {"sensor", "CompressedBytesWritten"},
                    {"filesystem", "test"},
                },
                [](i64 val) { return val > 0 && val < 370; } // expected
            },
        });
    }
}

}   // namespace NCloud::NFileStore::NStorage
