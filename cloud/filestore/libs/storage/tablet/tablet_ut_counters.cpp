#include "tablet.h"

#include <cloud/filestore/libs/diagnostics/metrics/registry.h>
#include <cloud/filestore/libs/diagnostics/metrics/visitor.h>
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
    ui32 maxReadBandwidth,
    ui32 maxWriteBandwidth,
    ui32 boostTime,
    ui32 boostRefillTime,
    ui32 boostPercentage,
    ui32 maxPostponedWeight,
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

////////////////////////////////////////////////////////////////////////////////

struct TEnv
    : public NUnitTest::TBaseFixture
{
    TTestEnv Env;
    std::unique_ptr<TIndexTabletClient> Tablet;

    TStringStream Data;
    NMetrics::IRegistryVisitorPtr Visitor;

    std::unique_ptr<TFileInput> FileStream;

    TEnv()
        : Visitor(NMetrics::CreateRegistryVisitor(Data))
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

    void SetupTestFile(const TString& fileName)
    {
        FileStream = std::make_unique<TFileInput>(
            GetWorkPath() + "/" + fileName + ".txt",
            OpenExisting | RdOnly | CloseOnExec);
    }

};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Counters)
{
    Y_UNIT_TEST_F(ShouldRegisterCounters, TEnv)
    {
        return; // FIXME NBS-4552

        SetupTestFile("ShouldRegisterCountersOnLoad");
        auto registry = Env.GetRegistry();

        registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(FileStream->ReadLine(), Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldCorrectlyWriteThrottlerMaxParams, TEnv)
    {
        return; // FIXME NBS-4552

        SetupTestFile("ShouldCorrectlyWriteThrottlerMaxParams");
        auto registry = Env.GetRegistry();

        registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(FileStream->ReadLine(), Data.Str());
        Data.Clear();

        Tablet->AdvanceTime(TDuration::Seconds(15));
        Env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));

        registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(FileStream->ReadLine(), Data.Str());
        Data.Clear();

        const auto config = MakeThrottlerConfig(
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

        registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(FileStream->ReadLine(), Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldIncrementAndDecrementSessionCount, TEnv)
    {
        return; // FIXME NBS-4552

        SetupTestFile("ShouldIncrementAndDecrementSessionCount");
        auto registry = Env.GetRegistry();

        registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(FileStream->ReadLine(), Data.Str());
        Data.Clear();

        Tablet->InitSession("client", "session");

        Tablet->AdvanceTime(TDuration::Seconds(15));
        Env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));

        registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(FileStream->ReadLine(), Data.Str());
        Data.Clear();

        Tablet->DestroySession();

        Tablet->AdvanceTime(TDuration::Seconds(15));
        Env.GetRuntime().DispatchEvents({}, TDuration::Seconds(5));

        registry->Visit(TInstant::Zero(), *Visitor);
        UNIT_ASSERT_VALUES_EQUAL(FileStream->ReadLine(), Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST(ShouldCorrectlyUpdateValuesOnThrottling)
    {
        return; // FIXME NBS-4552

        TStringStream data;
        NMetrics::IRegistryVisitorPtr visitor =
            NMetrics::CreateRegistryVisitor(data);

        TFileInput fileStream(
            GetWorkPath() + "/ShouldUpdateValuesOnThrottling.txt",
            OpenExisting | RdOnly | CloseOnExec);

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

        registry->Visit(TInstant::Zero(), *visitor);
        UNIT_ASSERT_VALUES_EQUAL(fileStream.ReadLine(), data.Str());
        data.Clear();

        // 1. Testing that excess requests are postponed.
        for (size_t i = 0; i < 20; ++i) {
            tablet.SendReadDataRequest(handle, 4_KB * i, 4_KB);
            tablet.AssertReadDataNoResponse();
        }

        registry->Visit(TInstant::Zero(), *visitor);
        UNIT_ASSERT_VALUES_EQUAL(fileStream.ReadLine(), data.Str());
        data.Clear();

        // Now we have 20_KB in PostponeQueue.

        for (size_t i = 0; i < 3; ++i) {
            tablet.SendWriteDataRequest(handle, 0, 4_KB, 'z');
            tablet.AssertWriteDataNoResponse();
        }

        registry->Visit(TInstant::Zero(), *visitor);
        UNIT_ASSERT_VALUES_EQUAL(fileStream.ReadLine(), data.Str());
        data.Clear();

        // Now we have 32_KB in PostponedQueue. Equal to MaxPostponedWeight.

        // 2. Testing that we start rejecting requests after
        // our postponed limit saturates.
        tablet.SendWriteDataRequest(handle, 0, 1, 'y');   // Event 1 byte request must be rejected.
        tablet.AssertWriteDataQuickResponse(E_FS_THROTTLED);
        tablet.SendReadDataRequest(handle, 0, 1_KB);
        tablet.AssertReadDataQuickResponse(E_FS_THROTTLED);

        registry->Visit(TInstant::Zero(), *visitor);
        UNIT_ASSERT_VALUES_EQUAL(fileStream.ReadLine(), data.Str());
        data.Clear();
    }
}

}   // namespace NCloud::NFileStore::NStorage
