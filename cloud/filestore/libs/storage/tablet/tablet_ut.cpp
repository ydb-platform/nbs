#include "tablet.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NMonitoring;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest)
{
    Y_UNIT_TEST(ShouldBootTablet)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.WaitReady();
    }

    Y_UNIT_TEST(ShouldUpdateConfig)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TDynamicCountersPtr counters = new TDynamicCounters();
        InitCriticalEventsCounter(counters);

        auto tabletUpdateConfigCounter = counters->GetCounter(
            "AppCriticalEvents/TabletUpdateConfigError",
            true);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        tablet.UpdateConfig({ .CloudId = "xxx" });
        UNIT_ASSERT_VALUES_EQUAL(0, tabletUpdateConfigCounter->Val());

        constexpr ui32 newChannelCount = DefaultChannelCount + 4;
        tablet.UpdateConfig({
            .CloudId = "xxx",
            .ChannelCount = newChannelCount,
            .StorageMediaKind = NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD});
        UNIT_ASSERT_VALUES_EQUAL(0, tabletUpdateConfigCounter->Val());
        {
            auto stats = GetStorageStats(tablet);
            UNIT_ASSERT_VALUES_EQUAL(
                newChannelCount,
                stats.GetConfigChannelCount());

            UNIT_ASSERT_EQUAL(
                NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD,
                GetStorageMediaKind(tablet));
        }
    }

    Y_UNIT_TEST(ShouldUpdatePerformanceProfileConfig)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        const auto nodeIdx = env.CreateNode("nfs");
        const auto tabletId = env.BootIndexTablet(nodeIdx);

        const auto counters = new TDynamicCounters();
        InitCriticalEventsCounter(counters);

        const auto tabletUpdateConfigCounter = counters->GetCounter(
            "AppCriticalEvents/TabletUpdateConfigError",
            true);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        constexpr bool throttlingEnabled = true;
        constexpr ui32 maxReadIops = 10;
        constexpr ui32 maxWriteIops = 15;
        constexpr ui64 maxReadBandwidth = 20;
        constexpr ui64 maxWriteBandwidth = 25;
        constexpr ui32 boostTime = 1'000; // 1 second
        constexpr ui32 boostRefillTime = 5'000; // 5 seconds
        constexpr ui32 boostPercentage = 75;
        constexpr ui64 maxPostponedWeight = 16_MB;
        constexpr ui32 maxWriteCostMultiplier = 15;
        constexpr ui32 maxPostponedTime = 7'000; // 7 seconds
        constexpr ui32 maxPostponedCount = 128;
        constexpr ui32 burstPercentage = 35;
        constexpr ui64 defaultPostponedRequestWeight = 3;

        tablet.UpdateConfig({
            .FileSystemId = "test_filesystem",
            .CloudId = "test_cloud",
            .FolderId = "test_folder",
            .PerformanceProfile = {
                .ThrottlingEnabled = throttlingEnabled,
                .MaxReadIops = maxReadIops,
                .MaxWriteIops = maxWriteIops,
                .MaxReadBandwidth = maxReadBandwidth,
                .MaxWriteBandwidth = maxWriteBandwidth,
                .BoostTime = boostTime,
                .BoostRefillTime = boostRefillTime,
                .BoostPercentage = boostPercentage,
                .MaxPostponedWeight = maxPostponedWeight,
                .MaxWriteCostMultiplier = maxWriteCostMultiplier,
                .MaxPostponedTime = maxPostponedTime,
                .MaxPostponedCount = maxPostponedCount,
                .BurstPercentage = burstPercentage,
                .DefaultPostponedRequestWeight = defaultPostponedRequestWeight
            }});

        auto performanceProfile =
            GetFileSystemConfig(tablet).GetPerformanceProfile();
        UNIT_ASSERT_VALUES_EQUAL(
            throttlingEnabled,
            performanceProfile.GetThrottlingEnabled());
        UNIT_ASSERT_VALUES_EQUAL(
            maxReadIops,
            performanceProfile.GetMaxReadIops());
        UNIT_ASSERT_VALUES_EQUAL(
            maxWriteIops,
            performanceProfile.GetMaxWriteIops());
        UNIT_ASSERT_VALUES_EQUAL(
            maxReadBandwidth,
            performanceProfile.GetMaxReadBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(
            maxWriteBandwidth,
            performanceProfile.GetMaxWriteBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(
            boostTime,
            performanceProfile.GetBoostTime());
        UNIT_ASSERT_VALUES_EQUAL(
            boostRefillTime,
            performanceProfile.GetBoostRefillTime());
        UNIT_ASSERT_VALUES_EQUAL(
            boostPercentage,
            performanceProfile.GetBoostPercentage());
        UNIT_ASSERT_VALUES_EQUAL(
            maxPostponedWeight,
            performanceProfile.GetMaxPostponedWeight());
        UNIT_ASSERT_VALUES_EQUAL(
            maxWriteCostMultiplier,
            performanceProfile.GetMaxWriteCostMultiplier());
        UNIT_ASSERT_VALUES_EQUAL(
            maxPostponedTime,
            performanceProfile.GetMaxPostponedTime());
        UNIT_ASSERT_VALUES_EQUAL(
            maxPostponedCount,
            performanceProfile.GetMaxPostponedCount());
        UNIT_ASSERT_VALUES_EQUAL(
            burstPercentage,
            performanceProfile.GetBurstPercentage());
        UNIT_ASSERT_VALUES_EQUAL(
            defaultPostponedRequestWeight,
            performanceProfile.GetDefaultPostponedRequestWeight());
    }

    Y_UNIT_TEST(ShouldFailToUpdateConfigIfValidationFails)
    {
        constexpr ui32 channelCount = 6;

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TDynamicCountersPtr counters = new TDynamicCounters();
        InitCriticalEventsCounter(counters);

        auto tabletUpdateConfigCounter = counters->GetCounter(
            "AppCriticalEvents/TabletUpdateConfigError",
            true);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId, {
            .ChannelCount = channelCount
        });

        // Should return OK status due to schemeshard exotic behaviour.
        tablet.UpdateConfig({
            .BlockSize = 4 * 4096, .ChannelCount = channelCount
        });
        UNIT_ASSERT_VALUES_EQUAL(1, tabletUpdateConfigCounter->Val());

        // BlockCount can actually be decreased without problems
        tablet.UpdateConfig({
            .BlockCount = 1, .ChannelCount = channelCount
        });
        UNIT_ASSERT_VALUES_EQUAL(1, tabletUpdateConfigCounter->Val());

        tablet.UpdateConfig({
            .ChannelCount = channelCount - 1
        });
        UNIT_ASSERT_VALUES_EQUAL(2, tabletUpdateConfigCounter->Val());
        {
            auto stats = GetStorageStats(tablet);
            UNIT_ASSERT_VALUES_EQUAL(
                channelCount,
                stats.GetConfigChannelCount());
        }
    }

    Y_UNIT_TEST(ShouldGetStorageConfig)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId, {});

        NProto::TStorageConfig patch;
        patch.SetMultiTabletForwardingEnabled(true);
        tablet.ChangeStorageConfig(std::move(patch));

        tablet.RebootTablet();

        auto response = tablet.GetStorageConfig();
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            response->Record.GetStorageConfig().GetMultiTabletForwardingEnabled());
    }

    Y_UNIT_TEST(ShouldNotifyServiceWhenFileSystemConfigChaged)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        const auto nodeIdx = env.CreateNode("nfs");
        const auto tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        ui64 registerNonShardCount = 0;
        ui64 registerShardCount = 0;
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvRegisterLocalFileStore: {
                        const auto* msg = event->template Get<
                            TEvService::TEvRegisterLocalFileStoreRequest>();
                        if (tabletId != msg->TabletId) {
                            break;
                        }
                        if (msg->IsShard) {
                           ++registerShardCount;
                        } else {
                            ++registerNonShardCount;
                        }
                    }
                }
                return false;
            });

        tablet.UpdateConfig({
            .FileSystemId = "test_filesystem",
            .CloudId = "test_cloud",
            .FolderId = "test_folder",
        });

        UNIT_ASSERT_VALUES_EQUAL(1, registerNonShardCount);
        UNIT_ASSERT_VALUES_EQUAL(0, registerShardCount);

        tablet.ConfigureAsShard(1);

        UNIT_ASSERT_VALUES_EQUAL(1, registerNonShardCount);
        UNIT_ASSERT_VALUES_EQUAL(1, registerShardCount);
    }
}

}   // namespace NCloud::NFileStore::NStorage
