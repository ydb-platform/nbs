#include "tablet.h"
#include "tablet_schema.h"

#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/maybe.h>
#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTablet
    : public NUnitTest::TBaseFixture
{
private:
    std::unique_ptr<TTestEnv> Env = nullptr;
    std::unique_ptr<TIndexTabletClient> Tablet = nullptr;
    ui64 Handle = 0;
    ui64 NodeId = 0;

public:
    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        InitTestEnvironment(true);
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        DestroyTestEnvironment();
    }

    void InitTestEnvironment(TMaybe<bool> throttlingEnabled)
    {
        if (Env) {
            DestroyTestEnvironment();
        }

        TTestEnvConfig envConfig;

        NProto::TStorageConfig storageConfig;
        if (throttlingEnabled.Defined()) {
            storageConfig.SetThrottlingEnabled(throttlingEnabled.GetRef());
            storageConfig.SetMultipleStageRequestThrottlingEnabled(
                throttlingEnabled.GetRef());
        }

        Env = std::make_unique<TTestEnv>(envConfig, storageConfig);
        Env->CreateSubDomain("nfs");
        const auto nodeIdx = Env->CreateNode("nfs");
        const auto tabletId = Env->BootIndexTablet(nodeIdx);

        Tablet = std::make_unique<TIndexTabletClient>(
            Env->GetRuntime(),
            nodeIdx,
            tabletId);
        Tablet->InitSession("client", "session");

        NodeId = CreateNode(*Tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        Handle = CreateHandle(*Tablet, NodeId);
    }

    auto UpdateConfig(const TFileSystemConfig& config)
    {
        return Tablet->UpdateConfig(config);
    }

    void WriteData(ui64 offset, ui32 len, char fill)
    {
        Tablet->SendWriteDataRequest(Handle, offset, len, fill);
    }

    void ReadData(ui64 offset, ui32 len)
    {
        Tablet->SendReadDataRequest(Handle, offset, len);
    }

    void DescribeData(ui64 offset, ui32 len)
    {
        Tablet->SendDescribeDataRequest(Handle, offset, len);
    }

    void GenerateBlobIds(ui64 offset, ui32 len)
    {
        Tablet->SendGenerateBlobIdsRequest(NodeId, Handle, offset, len);
    }

    void Tick(TDuration duration)
    {
        Tablet->AdvanceTime(duration);
    }

    void RebootTablet()
    {
        Tablet->RebootTablet();
        Tablet->RecoverSession();
    }

#define TEST_CLIENT_DECLARE_METHOD(name)                                       \
    auto Assert##name##QuickResponse(ui32 status)                              \
    {                                                                          \
        return Tablet->Assert##name##QuickResponse(status);                    \
    }                                                                          \
                                                                               \
    auto Assert##name##Response(ui32 status)                                   \
    {                                                                          \
        return Tablet->Assert##name##Response(status);                         \
    }                                                                          \
                                                                               \
    void Assert##name##NoResponse()                                            \
    {                                                                          \
        Tablet->Assert##name##NoResponse();                                    \
    }                                                                          \
// TEST_CLIENT_DECLARE_METHOD

    TEST_CLIENT_DECLARE_METHOD(ReadData);
    TEST_CLIENT_DECLARE_METHOD(WriteData);
    TEST_CLIENT_DECLARE_METHOD(DescribeData);
    TEST_CLIENT_DECLARE_METHOD(GenerateBlobIds);

#undef TEST_CLIENT_DECLARE_METHOD

private:
    void DestroyTestEnvironment()
    {
        Tablet->DestroyHandle(Handle);
    }
};

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
    ui32 defaultPostponedRequestWeight)
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Throttling)
{
    Y_UNIT_TEST_F(ShouldThrottle, TTablet)
    {
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
        UpdateConfig(config);

        // 0. Testing that at 1rps nothing is throttled.
        for (size_t i = 0; i < 10; ++i) {
            Tick(TDuration::Seconds(1));
            ReadData(4_KB * i, 4_KB);
            const auto readResponse =
                AssertReadDataQuickResponse(S_OK);

            if (i == 0) {
                UNIT_ASSERT_VALUES_EQUAL(
                    0,
                    readResponse->Record.GetBuffer().size());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(4_KB, static_cast<char>('a' + i - 1)),
                    readResponse->Record.GetBuffer());
            }

            Tick(TDuration::Seconds(1));
            WriteData(4_KB * (i + 1), 4_KB, static_cast<char>('a' + i));
            AssertWriteDataQuickResponse(S_OK);
        }

        // 1. Testing that excess requests are postponed.
        for (size_t i = 0; i < 20; ++i) {
            ReadData(4_KB * i, 4_KB);
            AssertReadDataNoResponse();
        }

        // Now we have 20_KB in PostponeQueue.

        for (size_t i = 0; i < 3; ++i) {
            WriteData(0, 4_KB, 'z');
            AssertWriteDataNoResponse();
        }

        // Now we have 32_KB in PostponedQueue. Equal to MaxPostponedWeight.

        // 2. Testing that we start rejecting requests after
        // our postponed limit saturates.
        WriteData(0, 1, 'y');   // Even 1 byte request must be rejected.
        AssertWriteDataQuickResponse(E_FS_THROTTLED);
        ReadData(0, 1_KB);
        AssertReadDataQuickResponse(E_FS_THROTTLED);

        // 3. Testing that after some time passes our postponed requests
        // are successfully processed.
        // Test actor runtime will automatically advance the timer for us.
        for (ui32 i = 0; i < 20; ++i) {
            Tick(TDuration::Seconds(1));
            const auto readResponse = AssertReadDataResponse(S_OK);
            if (i == 0) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(4_KB, '\0'),
                    readResponse->Record.GetBuffer());
            } else if (i <= 10) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(4_KB, static_cast<char>('a' + i - 1)),
                    readResponse->Record.GetBuffer());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    0,
                    readResponse->Record.GetBuffer().size());
            }
        }

        for (ui32 i = 0; i < 3; ++i) {
            Tick(TDuration::Seconds(1));
            AssertWriteDataResponse(S_OK);
        }

        // Now PostponedQueue is empty.

        // 4. Testing that bursts actually work.
        Tick(TDuration::Seconds(2));
        ReadData(0, 12_KB);
        {
            // Postponed WriteDataRequest must write 4_KB of 'z'.
            const auto readResponse = AssertReadDataQuickResponse(S_OK);
            UNIT_ASSERT_VALUES_EQUAL(
                TString(4_KB, 'z') + TString(4_KB, 'a') + TString(4_KB, 'b'),
                readResponse->Record.GetBuffer());
        }
        ReadData(12_KB, 4_KB);
        AssertReadDataNoResponse();
        {
            const auto readResponse = AssertReadDataResponse(S_OK);
            Tick(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(
                TString(4_KB, 'c'),
                readResponse->Record.GetBuffer());
        }

        // 5. Requests of any size should work, but not immediately.
        WriteData(0, 32_KB, 'a');
        Tick(TDuration::Seconds(5));
        AssertWriteDataResponse(S_OK);

        // TODO: 6. Test backpressure effect (NBS-2278).
        // TODO: 7. Test max count (NBS-2278).
    }

    Y_UNIT_TEST_F(ShouldThrottleMultipleStage, TTablet)
    {
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
            1_KB   // defaultPostponedRequestWeight
        );
        UpdateConfig(config);

        // 0. Testing that at 1rps nothing is throttled.
        for (size_t i = 0; i < 10; ++i) {
            Tick(TDuration::Seconds(1));
            DescribeData(4_KB * i, 4_KB);
            AssertDescribeDataQuickResponse(S_OK);
            Tick(TDuration::Seconds(1));
            GenerateBlobIds(4_KB * (i + 1), 4_KB);
            AssertGenerateBlobIdsQuickResponse(S_OK);
        }

        // 1. Testing that excess requests are postponed.
        for (size_t i = 0; i < 20; ++i) {
            DescribeData(4_KB * i, 4_KB);
            AssertDescribeDataNoResponse();
        }

        // Now we have 20_KB in PostponeQueue.

        for (size_t i = 0; i < 3; ++i) {
            GenerateBlobIds(0, 4_KB);
            AssertGenerateBlobIdsNoResponse();
        }

        // Now we have 32_KB in PostponedQueue. Equal to MaxPostponedWeight.

        // 2. Testing that we start rejecting requests after
        // our postponed limit saturates.

        GenerateBlobIds(0, config.BlockSize);   // Request must be rejected.
        AssertGenerateBlobIdsQuickResponse(E_FS_THROTTLED);
        DescribeData(0, config.BlockSize);
        AssertDescribeDataQuickResponse(E_FS_THROTTLED);

        // 3. Testing that after some time passes our postponed requests
        // are successfully processed.
        // Test actor runtime will automatically advance the timer for us.
        for (ui32 i = 0; i < 20; ++i) {
            Tick(TDuration::Seconds(1));
            AssertDescribeDataResponse(S_OK);
        }

        for (ui32 i = 0; i < 3; ++i) {
            Tick(TDuration::Seconds(1));
            AssertGenerateBlobIdsResponse(S_OK);
        }

        // Now PostponedQueue is empty.

        // 4. Testing that bursts actually work.
        Tick(TDuration::Seconds(2));
        DescribeData(0, 12_KB);
        {
            // Postponed WriteDataRequest must write 4_KB of 'z'.
            AssertDescribeDataQuickResponse(S_OK);
        }
        DescribeData(12_KB, 4_KB);
        AssertDescribeDataNoResponse();
        {
            AssertDescribeDataResponse(S_OK);
            Tick(TDuration::Seconds(1));
        }

        // 5. Requests of any size should work, but not immediately.
        GenerateBlobIds(0, 32_KB);
        Tick(TDuration::Seconds(5));
        AssertGenerateBlobIdsResponse(S_OK);
    }

    Y_UNIT_TEST_F(ShouldNotThrottleIfThrottlerIsDisabled, TTablet)
    {
        auto config = MakeThrottlerConfig(
            false,                                   // throttlingEnabled
            1,                                       // maxReadIops
            1,                                       // maxWriteIops
            4_KB,                                    // maxReadBandwidth
            4_KB,                                    // maxWriteBandwidth
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

        const auto checkFunction = [this](bool throttlerShouldBeEnabled) {
            WriteData(0, 4_KB, 'a');
            if (!throttlerShouldBeEnabled) {
                AssertWriteDataQuickResponse(S_OK);
            } else {
                AssertWriteDataNoResponse();
            }
        };

        // Default configs.
        InitTestEnvironment({});
        checkFunction(false);

        // Throttler is disabled in service.
        InitTestEnvironment(false);
        checkFunction(false);

        // Throttler is enabled in service.
        InitTestEnvironment(true);
        checkFunction(false);

        // Default service config with disabled throttler in FS config.
        InitTestEnvironment({});
        UpdateConfig(config);
        checkFunction(false);

        // Throttler is disabled in service, but enabled in FS config.
        InitTestEnvironment(false);
        UpdateConfig(config);
        checkFunction(false);

        // Throttler is enabled both in service and FS config.
        InitTestEnvironment(true);
        UpdateConfig(config);
        checkFunction(false);

        config.PerformanceProfile.ThrottlingEnabled = true;

        // Default service config with enabled throttler in FS config.
        InitTestEnvironment({});
        UpdateConfig(config);
        checkFunction(false);

        // Throttler is disabled in service, but enabled in FS config.
        InitTestEnvironment(false);
        UpdateConfig(config);
        checkFunction(false);

        // Throttler is enabled both in service and FS config.
        InitTestEnvironment(true);
        UpdateConfig(config);
        checkFunction(true);
    }

    Y_UNIT_TEST_F(ShouldRespondToThrottledRequestsUponTableDeath, TTablet)
    {
        auto config = MakeThrottlerConfig(
            true,                                    // throttlingEnabled
            1,                                       // maxReadIops
            1,                                       // maxWriteIops
            4_KB,                                    // maxReadBandwidth
            4_KB,                                    // maxWriteBandwidth
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
        UpdateConfig(config);

        ReadData(0, 8_KB);
        AssertReadDataNoResponse();

        RebootTablet();

        AssertReadDataQuickResponse(E_REJECTED);
    }

    Y_UNIT_TEST_F(ShouldProperlyProcessPostponedRequestsAfterConfigUpdate, TTablet)
    {
        auto config = MakeThrottlerConfig(
            true,                                    // throttlingEnabled
            1,                                       // maxReadIops
            1,                                       // maxWriteIops
            4_KB,                                    // maxReadBandwidth
            4_KB,                                    // maxWriteBandwidth
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
        UpdateConfig(config);

        ReadData(0, 8_KB);
        AssertReadDataNoResponse();

        // After UpdateConfig call, version should increment, so request will
        // be proceeded immediately.
        config.PerformanceProfile.ThrottlingEnabled = true;
        UpdateConfig(config);

        AssertReadDataResponse(S_OK);

        ReadData(0, 8_KB);
        AssertReadDataNoResponse();
    }

    Y_UNIT_TEST_F(ShouldMaintainRequestOrderWhenThrottling, TTablet)
    {
        auto config = MakeThrottlerConfig(
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
        UpdateConfig(config);

        ReadData(0, 1);
        AssertReadDataQuickResponse(S_OK);

        ReadData(0, 16_KB);
        AssertReadDataNoResponse();

        // Small write request should not be able to bypass the large one,
        // that was sent earlier.
        WriteData(0, 1, 'a');
        AssertWriteDataNoResponse();
        AssertReadDataNoResponse();

        Tick(TDuration::Seconds(1));
        AssertWriteDataNoResponse();
        AssertReadDataNoResponse();

        const auto response = AssertReadDataResponse(S_OK);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            response->Record.GetBuffer().size());
        AssertWriteDataResponse(S_OK);
    }

    Y_UNIT_TEST_F(ShouldRejectRequestsThrottledForTooLong, TTablet)
    {
        auto config = MakeThrottlerConfig(
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
            TDuration::Seconds(1).MilliSeconds(),    // maxPostponedTime
            64,                                      // maxPostponedCount
            100,                                     // burstPercentage
            1_KB                                     // defaultPostponedRequestWeight
        );
        UpdateConfig(config);

        // Has delay 2s, but budget is 1s => resulting delay is 1s.
        ReadData(0, 12_KB);
        AssertReadDataNoResponse();

        // Will stay in queue at least 1s + some delay => more than limit.
        WriteData(0, 1, 'a');
        AssertReadDataNoResponse();

        // First request should be proceeded, the second one is throttled.
        AssertReadDataResponse(S_OK);
        AssertWriteDataResponse(E_FS_THROTTLED);
    }

    Y_UNIT_TEST_F(ShouldUpdateThrottlerDelayOnReadRequest, TTablet)
    {
        auto config = MakeThrottlerConfig(
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
            TDuration::Seconds(1).MilliSeconds(),    // maxPostponedTime
            64,                                      // maxPostponedCount
            100,                                     // burstPercentage
            1_KB                                     // defaultPostponedRequestWeight
        );
        UpdateConfig(config);

        // Has delay 2s, but budget is 1s => resulting delay is 1s.
        ReadData(0, 12_KB);
        AssertReadDataNoResponse();

        const auto resp = AssertReadDataResponse(S_OK);
        UNIT_ASSERT(resp->Record.GetHeaders().GetThrottler().GetDelay() > 0);
    }

    Y_UNIT_TEST_F(ShouldUpdateThrottlerDelayOnWriteRequest, TTablet)
    {
        auto config = MakeThrottlerConfig(
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
            TDuration::Seconds(1).MilliSeconds(),    // maxPostponedTime
            64,                                      // maxPostponedCount
            100,                                     // burstPercentage
            1_KB                                     // defaultPostponedRequestWeight
        );
        UpdateConfig(config);

        // Has delay 2s, but budget is 1s => resulting delay is 1s.
        WriteData(0, 12_KB, 'a');
        AssertWriteDataNoResponse();

        const auto resp = AssertWriteDataResponse(S_OK);
        UNIT_ASSERT(resp->Record.GetHeaders().GetThrottler().GetDelay() > 0);
    }

    TABLET_TEST(ShouldAutomaticallyRunCleanupForSparselyPopulatedRanges)
    {
        const auto block = tabletConfig.BlockSize;
        const auto fileSize = 16_MB;
        const auto blockCount = fileSize / block;

        // Disable automatic cleanup
        NProto::TStorageConfig storageConfig;
        storageConfig.SetNewCleanupEnabled(true);
        storageConfig.SetCleanupThresholdAverage(999'999);
        storageConfig.SetWriteBlobThreshold(block);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto nodeId = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, nodeId);
        tablet.WriteData(handle, 0, fileSize, 'a');
        tablet.DestroyHandle(handle);

        TSetNodeAttrArgs args(nodeId);
        args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
        args.SetSize(block);
        tablet.SetNodeAttr(args);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2 * blockCount - 1,
                stats.GetDeletionMarkersCount());
        }

        // Enable automatic cleanup and configure throttling to consume 1% of CPU
        storageConfig.SetCleanupThresholdAverage(64);
        storageConfig.SetCleanupCpuThrottlingThreshold(1);
        storageConfig.SetCalculateCleanupScoreBasedOnUsedBlocksCount(true);
        tablet.ChangeStorageConfig(storageConfig);
        tablet.RebootTablet();
        tablet.InitSession("client", "session");

        // Cleanup is expected to run a bit and stop because of throttling
        ui64 deletionMarkersCount = 0;
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetUsedBlocksCount());
            deletionMarkersCount = stats.GetDeletionMarkersCount();
            UNIT_ASSERT_LT(0, deletionMarkersCount);
            UNIT_ASSERT_GT(2 * blockCount - 1, deletionMarkersCount);
        }

        // Advance time and trigger cleanup again by calling a user operation
        tablet.AdvanceTime(TDuration::Seconds(1));
        tablet.SetNodeAttr(args);

        // Cleanup is expected to process a bit more blocks
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetUsedBlocksCount());
            UNIT_ASSERT_GT(deletionMarkersCount,
                stats.GetDeletionMarkersCount());
        }

        // Disable cleanup and spawn deletion markers again
        storageConfig.SetCleanupThresholdAverage(999'999);
        storageConfig.SetCalculateCleanupScoreBasedOnUsedBlocksCount(false);
        tablet.ChangeStorageConfig(storageConfig);
        tablet.RebootTablet();
        tablet.InitSession("client", "session");

        handle = CreateHandle(tablet, nodeId);
        tablet.WriteData(handle, 0, fileSize, 'a');
        tablet.DestroyHandle(handle);
        tablet.SetNodeAttr(args);

        // Enable cleanup and configure throttling to consume 100% of CPU
        storageConfig.SetCleanupThresholdAverage(64);
        storageConfig.SetCleanupCpuThrottlingThreshold(100);
        storageConfig.SetCalculateCleanupScoreBasedOnUsedBlocksCount(true);
        tablet.ChangeStorageConfig(storageConfig);
        tablet.RebootTablet();

        // Cleanup is expected to process all the blocks
        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeletionMarkersCount());
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
