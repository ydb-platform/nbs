#include "config.h"

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/ydb/core/control/immediate_control_board_impl.h>

#include <util/generic/vector.h>

#include <latch>
#include <thread>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConfigTest)
{
    // Check that shared controls expose compiled defaults on the native board
    // and treat an explicit equal-to-default value as no override.
    Y_UNIT_TEST(ShouldUseCompiledDefaultsForSharedControls)
    {
        // Observe the registered control through another wrapper on the board.
        auto controls = std::make_shared<TStorageConfigControls>();
        NKikimr::TControlBoard controlBoard;
        controls->Register(controlBoard);
        NKikimr::TControlWrapper control;
        UNIT_ASSERT(!controlBoard.RegisterSharedControl(
            control,
            "BlockStore_WriteBlobThreshold"));
        const TStorageConfig defaultConfig({}, nullptr);
        const auto defaultValue = defaultConfig.GetWriteBlobThreshold();
        UNIT_ASSERT_VALUES_EQUAL(defaultValue, control.GetDefault());
        UNIT_ASSERT_VALUES_EQUAL(defaultValue, static_cast<i64>(control));

        // Keep a distinct raw value to expose the equal-to-default limitation.
        NProto::TStorageServiceConfig proto;
        proto.SetWriteBlobThreshold(defaultValue + 1);
        const TStorageConfig config(proto, nullptr, controls);
        TAtomic previousValue = {};
        controlBoard.SetValue(
            "BlockStore_WriteBlobThreshold",
            defaultValue,
            previousValue);
        UNIT_ASSERT(!controls->GetOverride("WriteBlobThreshold"));
        UNIT_ASSERT_VALUES_EQUAL(
            defaultValue + 1,
            config.GetWriteBlobThreshold());
    }

    // Check native defaults, explicit zero and false, compiled fallback, and
    // Restore while retaining a snapshot with its own raw values.
    Y_UNIT_TEST(ShouldUpdateDefaultsWithoutChangingRetainedProto)
    {
        // Initialize the shared controls from a startup configuration.
        auto controls = std::make_shared<TStorageConfigControls>();
        NProto::TStorageServiceConfig proto;
        proto.SetWriteBlobThreshold(100);
        proto.SetHiveLockExpireTimeout(4321);
        proto.SetHiveProxyFallbackMode(true);
        controls->UpdateDefaults(proto);
        const TStorageConfig retained(proto, nullptr, controls);
        NKikimr::TControlBoard board;
        controls->Register(board);
        NKikimr::TControlWrapper threshold;
        NKikimr::TControlWrapper timeout;
        NKikimr::TControlWrapper fallbackMode;
        UNIT_ASSERT(!board.RegisterSharedControl(
            threshold,
            "BlockStore_WriteBlobThreshold"));
        UNIT_ASSERT(!board.RegisterSharedControl(
            timeout,
            "BlockStore_HiveLockExpireTimeout"));
        UNIT_ASSERT(!board.RegisterSharedControl(
            fallbackMode,
            "BlockStore_HiveProxyFallbackMode"));
        UNIT_ASSERT_VALUES_EQUAL(100, threshold.GetDefault());
        UNIT_ASSERT_VALUES_EQUAL(4321, timeout.GetDefault());
        UNIT_ASSERT_VALUES_EQUAL(1, fallbackMode.GetDefault());

        // Update Default even when the operator already selected the new base.
        TAtomic previousValue = {};
        board.SetValue("BlockStore_WriteBlobThreshold", 200, previousValue);
        proto.SetWriteBlobThreshold(200);
        controls->UpdateDefaults(proto);
        UNIT_ASSERT_VALUES_EQUAL(200, threshold.GetDefault());
        UNIT_ASSERT(!controls->GetOverride("WriteBlobThreshold"));
        UNIT_ASSERT_VALUES_EQUAL(100, retained.GetWriteBlobThreshold());

        // Preserve an override on an unchanged base, then restore only Value.
        board.SetValue("BlockStore_WriteBlobThreshold", 300, previousValue);
        controls->UpdateDefaults(proto);
        UNIT_ASSERT_VALUES_EQUAL(300, retained.GetWriteBlobThreshold());
        UNIT_ASSERT(controls->RestoreDefault("WriteBlobThreshold"));
        UNIT_ASSERT_VALUES_EQUAL(200, threshold.GetDefault());
        UNIT_ASSERT_VALUES_EQUAL(200, static_cast<i64>(threshold));
        UNIT_ASSERT_VALUES_EQUAL(100, retained.GetWriteBlobThreshold());

        // Honor explicit zero and false instead of compiled defaults.
        proto.SetWriteBlobThreshold(0);
        proto.SetHiveLockExpireTimeout(0);
        proto.SetHiveProxyFallbackMode(false);
        controls->UpdateDefaults(proto);
        UNIT_ASSERT_VALUES_EQUAL(0, threshold.GetDefault());
        UNIT_ASSERT_VALUES_EQUAL(0, timeout.GetDefault());
        UNIT_ASSERT_VALUES_EQUAL(0, fallbackMode.GetDefault());

        // Recover compiled defaults when the raw configuration omits fields.
        controls->UpdateDefaults({});
        const TStorageConfig defaults({}, nullptr);
        UNIT_ASSERT_VALUES_EQUAL(
            defaults.GetWriteBlobThreshold(),
            threshold.GetDefault());
        UNIT_ASSERT_VALUES_EQUAL(
            defaults.GetHiveLockExpireTimeout().MilliSeconds(),
            timeout.GetDefault());
        UNIT_ASSERT_VALUES_EQUAL(100, retained.GetWriteBlobThreshold());
    }

    Y_UNIT_TEST(ShouldUpdateHiveProxyFallbackModeViaImmediateControlBoard)
    {
        auto config = std::make_shared<TStorageConfig>(
            NProto::TStorageServiceConfig{},
            std::make_shared<NFeatures::TFeaturesConfig>());
        NKikimr::TControlBoard controlBoard;
        config->Register(controlBoard);

        UNIT_ASSERT(!config->GetHiveProxyFallbackMode());

        TAtomic previousValue = {};
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_HiveProxyFallbackMode",
            1,
            previousValue));
        UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(previousValue));
        UNIT_ASSERT(config->GetHiveProxyFallbackMode());
    }

    // Check that controls created by a config can be reused with other protos,
    // while Restore exposes each config's own raw value.
    Y_UNIT_TEST(ShouldReuseAutomaticallyCreatedControlsViaIcb)
    {
        // Verify that ICB overrides all configurations when one configuration
        // default equals the ICB value and the other two defaults differ.
        NProto::TStorageServiceConfig firstProto;
        firstProto.SetWriteBlobThreshold(100);
        auto first = std::make_shared<TStorageConfig>(
            firstProto,
            std::make_shared<NFeatures::TFeaturesConfig>());
        auto controls = first->GetStorageConfigControls();
        UNIT_ASSERT(controls);

        NProto::TStorageServiceConfig secondProto;
        secondProto.SetWriteBlobThreshold(200);
        auto second = std::make_shared<TStorageConfig>(
            secondProto,
            std::make_shared<NFeatures::TFeaturesConfig>(),
            controls);
        UNIT_ASSERT(second->GetStorageConfigControls() == controls);

        NProto::TStorageServiceConfig thirdProto;
        thirdProto.SetWriteBlobThreshold(300);
        auto third = std::make_shared<TStorageConfig>(
            thirdProto,
            std::make_shared<NFeatures::TFeaturesConfig>(),
            controls);
        UNIT_ASSERT(third->GetStorageConfigControls() == controls);

        NKikimr::TControlBoard controlBoard;
        first->Register(controlBoard);
        second->Register(controlBoard);
        third->Register(controlBoard);

        TAtomic previousValue = {};
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_WriteBlobThreshold",
            200,
            previousValue));
        UNIT_ASSERT_VALUES_EQUAL(100, AtomicGet(previousValue));
        UNIT_ASSERT_VALUES_EQUAL(200, first->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(200, second->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(200, third->GetWriteBlobThreshold());

        controlBoard.RestoreDefault("BlockStore_WriteBlobThreshold");
        UNIT_ASSERT_VALUES_EQUAL(100, first->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(200, second->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(300, third->GetWriteBlobThreshold());
    }

    // Check explicit control sharing and per-config raw fallback after Restore.
    Y_UNIT_TEST(ShouldSetAndRestoreSharedControlsViaIcb)
    {
        // Verify that ICB overrides all configurations when one configuration
        // default equals the ICB value and the other two defaults differ.
        auto controls = std::make_shared<TStorageConfigControls>();
        UNIT_ASSERT(!controls->GetOverride("WriteBlobThreshold"));
        UNIT_ASSERT(!controls->RestoreDefault("WriteBlobThreshold"));

        NKikimr::TControlBoard controlBoard;
        controls->Register(controlBoard);
        controls->Register(controlBoard);

        NProto::TStorageServiceConfig firstProto;
        firstProto.SetWriteBlobThreshold(100);
        auto first = std::make_shared<TStorageConfig>(
            firstProto,
            std::make_shared<NFeatures::TFeaturesConfig>(),
            controls);
        UNIT_ASSERT(first->GetStorageConfigControls() == controls);

        NProto::TStorageServiceConfig secondProto;
        secondProto.SetWriteBlobThreshold(200);
        auto second = std::make_shared<TStorageConfig>(
            secondProto,
            std::make_shared<NFeatures::TFeaturesConfig>(),
            controls);
        UNIT_ASSERT(second->GetStorageConfigControls() == controls);

        NProto::TStorageServiceConfig thirdProto;
        thirdProto.SetWriteBlobThreshold(300);
        auto third = std::make_shared<TStorageConfig>(
            thirdProto,
            std::make_shared<NFeatures::TFeaturesConfig>(),
            controls);
        UNIT_ASSERT(third->GetStorageConfigControls() == controls);

        UNIT_ASSERT_VALUES_EQUAL(100, first->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(200, second->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(300, third->GetWriteBlobThreshold());

        TAtomic previousValue = {};
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_WriteBlobThreshold",
            200,
            previousValue));

        const auto override = controls->GetOverride("WriteBlobThreshold");
        UNIT_ASSERT(override);
        UNIT_ASSERT_VALUES_EQUAL(200, *override);
        UNIT_ASSERT_VALUES_EQUAL(200, first->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(200, second->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(200, third->GetWriteBlobThreshold());

        UNIT_ASSERT(controls->RestoreDefault("WriteBlobThreshold"));
        UNIT_ASSERT(!controls->GetOverride("WriteBlobThreshold"));
        UNIT_ASSERT_VALUES_EQUAL(100, first->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(200, second->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(300, third->GetWriteBlobThreshold());
        UNIT_ASSERT(!controls->RestoreDefault("UnknownField"));
    }

    // Check that concurrent registration of the same controls is idempotent.
    Y_UNIT_TEST(ShouldRegisterSharedControlsConcurrently)
    {
        auto controls = std::make_shared<TStorageConfigControls>();
        NKikimr::TControlBoard controlBoard;

        constexpr int ThreadCount = 4;
        std::latch start{ThreadCount + 1};
        TVector<std::thread> threads;
        for (int i = 0; i != ThreadCount; ++i) {
            threads.emplace_back(
                [&]
                {
                    start.arrive_and_wait();
                    controls->Register(controlBoard);
                });
        }

        start.arrive_and_wait();
        for (auto& thread: threads) {
            thread.join();
        }

        TAtomic previousValue = {};
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_WriteBlobThreshold",
            100,
            previousValue));

        const auto override = controls->GetOverride("WriteBlobThreshold");
        UNIT_ASSERT(override);
        UNIT_ASSERT_VALUES_EQUAL(100, *override);
    }

    // Check that copies keep live controls and independent raw protos after
    // destroying the original config, with explicit or automatic controls.
    Y_UNIT_TEST(ShouldCopyConfigAndShareControlsViaIcb)
    {
        const auto test = [](TStorageConfigControlsPtr controls) {
            NProto::TStorageServiceConfig proto;
            proto.SetWriteBlobThreshold(100);
            proto.SetVolumePreemptionType(NProto::PREEMPTION_NONE);

            NKikimr::TControlBoard controlBoard;
            TStorageConfigPtr copy;
            {
                auto source = std::make_shared<TStorageConfig>(
                    proto,
                    std::make_shared<NFeatures::TFeaturesConfig>(),
                    controls);
                source->Register(controlBoard);
                controls = source->GetStorageConfigControls();

                TAtomic previousValue = {};
                UNIT_ASSERT(!controlBoard.SetValue(
                    "BlockStore_WriteBlobThreshold",
                    200,
                    previousValue));

                copy = std::make_shared<TStorageConfig>(*source);
                UNIT_ASSERT_VALUES_EQUAL(200, copy->GetWriteBlobThreshold());
                UNIT_ASSERT(copy->GetStorageConfigControls() == controls);

                source->SetVolumePreemptionType(
                    NProto::PREEMPTION_MOVE_MOST_HEAVY);
                UNIT_ASSERT(
                    copy->GetVolumePreemptionType() == NProto::PREEMPTION_NONE);
            }

            TAtomic previousValue = {};
            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_WriteBlobThreshold",
                300,
                previousValue));
            UNIT_ASSERT_VALUES_EQUAL(200, AtomicGet(previousValue));
            UNIT_ASSERT_VALUES_EQUAL(300, copy->GetWriteBlobThreshold());

            controlBoard.RestoreDefault("BlockStore_WriteBlobThreshold");
            UNIT_ASSERT_VALUES_EQUAL(100, copy->GetWriteBlobThreshold());
        };

        test(std::make_shared<TStorageConfigControls>());
        test(nullptr);
    }

    // Check that patching a config with automatically created controls keeps
    // live ICB overrides and restores patched raw values.
    Y_UNIT_TEST(ShouldMergeAutomaticallyCreatedControlsViaIcb)
    {
        NProto::TStorageServiceConfig globalConfigProto;
        globalConfigProto.SetMaxMigrationIoDepth(4);
        auto globalConfig = std::make_shared<const TStorageConfig>(
            globalConfigProto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        NKikimr::TControlBoard controlBoard;
        globalConfig->Register(controlBoard);

        TAtomic previousValue = {};
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_MaxMigrationIoDepth",
            8,
            previousValue));

        // Keep ICB above the patch without copying the override into its proto.
        NProto::TStorageServiceConfig patch;
        patch.SetMaxMigrationIoDepth(1);
        auto config = TStorageConfig::Merge(globalConfig, patch);

        UNIT_ASSERT_UNEQUAL(config, globalConfig);
        UNIT_ASSERT(config->GetStorageConfigControls());
        UNIT_ASSERT(
            config->GetStorageConfigControls() ==
            globalConfig->GetStorageConfigControls());
        UNIT_ASSERT_VALUES_EQUAL(8, config->GetMaxMigrationIoDepth());
        UNIT_ASSERT_VALUES_EQUAL(
            8,
            config->GetEffectiveStorageConfigProto().GetMaxMigrationIoDepth());

        // Observe later operator writes through both configurations.
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_MaxMigrationIoDepth",
            16,
            previousValue));
        UNIT_ASSERT_VALUES_EQUAL(16, globalConfig->GetMaxMigrationIoDepth());
        UNIT_ASSERT_VALUES_EQUAL(16, config->GetMaxMigrationIoDepth());

        // Restore each configuration's own raw value.
        controlBoard.RestoreDefault("BlockStore_MaxMigrationIoDepth");
        UNIT_ASSERT_VALUES_EQUAL(4, globalConfig->GetMaxMigrationIoDepth());
        UNIT_ASSERT_VALUES_EQUAL(1, config->GetMaxMigrationIoDepth());
    }

    // Check that Merge retains supplied controls and applies ICB above a patch.
    Y_UNIT_TEST(ShouldMergeSuppliedControlsViaIcb)
    {
        auto controls = std::make_shared<TStorageConfigControls>();
        NKikimr::TControlBoard controlBoard;
        controls->Register(controlBoard);

        NProto::TStorageServiceConfig globalConfigProto;
        globalConfigProto.SetMaxMigrationIoDepth(4);
        auto globalConfig = std::make_shared<TStorageConfig>(
            globalConfigProto,
            std::make_shared<NFeatures::TFeaturesConfig>(),
            controls);

        TAtomic previousValue = {};
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_MaxMigrationIoDepth",
            8,
            previousValue));

        NProto::TStorageServiceConfig patch;
        patch.SetMaxMigrationIoDepth(1);
        auto config = TStorageConfig::Merge(globalConfig, patch);

        UNIT_ASSERT_UNEQUAL(config, globalConfig);
        UNIT_ASSERT(config->GetStorageConfigControls() == controls);
        UNIT_ASSERT_VALUES_EQUAL(8, config->GetMaxMigrationIoDepth());
        UNIT_ASSERT_VALUES_EQUAL(
            8,
            config->GetEffectiveStorageConfigProto().GetMaxMigrationIoDepth());

        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_MaxMigrationIoDepth",
            16,
            previousValue));
        UNIT_ASSERT_VALUES_EQUAL(16, globalConfig->GetMaxMigrationIoDepth());
        UNIT_ASSERT_VALUES_EQUAL(16, config->GetMaxMigrationIoDepth());

        UNIT_ASSERT(controls->RestoreDefault("MaxMigrationIoDepth"));
        UNIT_ASSERT_VALUES_EQUAL(4, globalConfig->GetMaxMigrationIoDepth());
        UNIT_ASSERT_VALUES_EQUAL(1, config->GetMaxMigrationIoDepth());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            config->GetEffectiveStorageConfigProto().GetMaxMigrationIoDepth());
    }

    Y_UNIT_TEST(ShouldOverrideConfigFields)
    {
        NProto::TStorageServiceConfig globalConfigProto;
        globalConfigProto.SetMaxMigrationBandwidth(100);
        globalConfigProto.SetMaxMigrationIoDepth(4);

        NProto::TStorageServiceConfig patch;
        patch.SetMaxMigrationBandwidth(400);

        auto globalConfig = std::make_shared<TStorageConfig>(
            globalConfigProto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        auto config = TStorageConfig::Merge(globalConfig, patch);
        UNIT_ASSERT_UNEQUAL(config, globalConfig);

        UNIT_ASSERT_VALUES_EQUAL(
            patch.GetMaxMigrationBandwidth(),
            config->GetMaxMigrationBandwidth());

        UNIT_ASSERT_VALUES_EQUAL(
            globalConfigProto.GetMaxMigrationIoDepth(),
            config->GetMaxMigrationIoDepth());

        UNIT_ASSERT_VALUES_EQUAL("/Root", config->GetSchemeShardDir());
    }

    Y_UNIT_TEST(ShouldIgnoreEmptyPatch)
    {
        NProto::TStorageServiceConfig globalConfigProto;
        globalConfigProto.SetMaxMigrationBandwidth(100);

        NProto::TStorageServiceConfig patch;

        auto globalConfig = std::make_shared<TStorageConfig>(
            globalConfigProto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        auto config = TStorageConfig::Merge(globalConfig, patch);
        UNIT_ASSERT_EQUAL(globalConfig, config);

        UNIT_ASSERT_VALUES_EQUAL(
            globalConfigProto.GetMaxMigrationBandwidth(),
            config->GetMaxMigrationBandwidth());

        UNIT_ASSERT_VALUES_EQUAL("/Root", config->GetSchemeShardDir());
    }

    Y_UNIT_TEST(ShouldOverrideConfigsViaImmediateControlBoard)
    {
        const auto defaultConfig = std::make_shared<TStorageConfig>(
            NProto::TStorageServiceConfig{},
            std::make_shared<NFeatures::TFeaturesConfig>());

        NKikimr::TControlBoard controlBoard;

        const NProto::TStorageServiceConfig globalConfigProto = [] {;
            NProto::TStorageServiceConfig proto;
            proto.SetMaxMigrationBandwidth(100);
            proto.SetMaxMigrationIoDepth(4);
            return proto;
        } ();

        auto globalConfig = std::make_shared<TStorageConfig>(
            globalConfigProto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        globalConfig->Register(controlBoard);

        UNIT_ASSERT_VALUES_EQUAL(
            globalConfigProto.GetMaxMigrationBandwidth(),
            globalConfig->GetMaxMigrationBandwidth());

        UNIT_ASSERT_VALUES_EQUAL(
            globalConfigProto.GetMaxMigrationIoDepth(),
            globalConfig->GetMaxMigrationIoDepth());

        UNIT_ASSERT_VALUES_EQUAL(
            defaultConfig->GetExpectedDiskAgentSize(),
            globalConfig->GetExpectedDiskAgentSize());

        UNIT_ASSERT_VALUES_EQUAL(
            defaultConfig->GetSchemeShardDir(),
            globalConfig->GetSchemeShardDir());

        // override MaxMigrationBandwidth via ICB

        const ui32 maxMigrationBandwidthICB = 400;

        {
            TAtomic prevValue = {};
            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_MaxMigrationBandwidth",
                maxMigrationBandwidthICB,
                prevValue));

            UNIT_ASSERT_VALUES_EQUAL(
                globalConfigProto.GetMaxMigrationBandwidth(),
                AtomicGet(prevValue));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            maxMigrationBandwidthICB,
            globalConfig->GetMaxMigrationBandwidth());

        // override MaxMigrationIoDepth via ICB

        const ui32 maxMigrationIoDepthICB = 8;

        {
            TAtomic prevValue = {};
            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_MaxMigrationIoDepth",
                maxMigrationIoDepthICB,
                prevValue));

            UNIT_ASSERT_VALUES_EQUAL(
                globalConfigProto.GetMaxMigrationIoDepth(),
                AtomicGet(prevValue));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            maxMigrationIoDepthICB,
            globalConfig->GetMaxMigrationIoDepth());

        const auto effectiveProto =
            globalConfig->GetEffectiveStorageConfigProto();
        UNIT_ASSERT_VALUES_EQUAL(
            maxMigrationBandwidthICB,
            effectiveProto.GetMaxMigrationBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(
            maxMigrationIoDepthICB,
            effectiveProto.GetMaxMigrationIoDepth());

        // Apply a patch with new MaxMigrationIoDepth & ExpectedDiskAgentSize

        const ui32 maxMigrationIoDepthPatch = 1;
        const ui32 expectedDiskAgentSizePatch = 100;

        NProto::TStorageServiceConfig patch;
        patch.SetMaxMigrationIoDepth(maxMigrationIoDepthPatch);
        patch.SetExpectedDiskAgentSize(expectedDiskAgentSizePatch);

        auto config = TStorageConfig::Merge(globalConfig, patch);
        UNIT_ASSERT_UNEQUAL(globalConfig, config);

        UNIT_ASSERT_VALUES_EQUAL(
            maxMigrationBandwidthICB,
            config->GetMaxMigrationBandwidth());

        UNIT_ASSERT_VALUES_EQUAL(
            maxMigrationIoDepthICB,
            config->GetMaxMigrationIoDepth());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedDiskAgentSizePatch,
            config->GetExpectedDiskAgentSize());

        UNIT_ASSERT_VALUES_EQUAL(
            defaultConfig->GetSchemeShardDir(),
            config->GetSchemeShardDir());
    }

    Y_UNIT_TEST(ShouldOverrideConfigsViaImmediateControlBoard2)
    {
        // Check for simple overrides.
        {
            NProto::TStorageServiceConfig overriddenProto = []
            {
                NProto::TStorageServiceConfig proto;
                proto.SetMaxMigrationBandwidth(400);
                proto.SetDefaultTabletVersion(1);
                return proto;
            }();
            const auto overriddenConfig = std::make_shared<TStorageConfig>(
                std::move(overriddenProto),
                std::make_shared<NFeatures::TFeaturesConfig>());

            UNIT_ASSERT_VALUES_EQUAL(
                400,
                overriddenConfig->GetMaxMigrationBandwidth());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                overriddenConfig->GetMaxMigrationIoDepth());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                overriddenConfig->GetDefaultTabletVersion());
            UNIT_ASSERT_VALUES_EQUAL(
                TDuration::Minutes(1),
                overriddenConfig
                    ->GetNonReplicatedAgentDisconnectRecoveryInterval());
            UNIT_ASSERT_EQUAL(
                NCloud::NProto::AUTHORIZATION_IGNORE,
                overriddenConfig->GetAuthorizationMode());

            NKikimr::TControlBoard controlBoard;
            overriddenConfig->Register(controlBoard);

            UNIT_ASSERT_VALUES_EQUAL(
                400,
                overriddenConfig->GetMaxMigrationBandwidth());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                overriddenConfig->GetDefaultTabletVersion());

            TAtomic prevValue{};
            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_MaxMigrationBandwidth",
                600,
                prevValue));
            UNIT_ASSERT_VALUES_EQUAL(
                600,
                overriddenConfig->GetMaxMigrationBandwidth());

            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_MaxMigrationBandwidth",
                0,
                prevValue));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                overriddenConfig->GetMaxMigrationBandwidth());

            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_DefaultTabletVersion",
                0,
                prevValue));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                overriddenConfig->GetDefaultTabletVersion());

            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_AuthorizationMode",
                2,
                prevValue));
            UNIT_ASSERT_EQUAL(
                NCloud::NProto::AUTHORIZATION_REQUIRE,
                overriddenConfig->GetAuthorizationMode());
        }

        // Check with zeroed field in the proto config.
        {
            NProto::TStorageServiceConfig overriddenProto = []
            {
                NProto::TStorageServiceConfig proto;
                proto.SetMaxMigrationBandwidth(0);
                proto.SetAuthorizationMode(
                    NCloud::NProto::AUTHORIZATION_ACCEPT);
                return proto;
            }();
            const auto overriddenConfig = std::make_shared<TStorageConfig>(
                std::move(overriddenProto),
                std::make_shared<NFeatures::TFeaturesConfig>());

            UNIT_ASSERT_VALUES_EQUAL(
                0,
                overriddenConfig->GetMaxMigrationBandwidth());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                overriddenConfig->GetDefaultTabletVersion());
            UNIT_ASSERT_EQUAL(
                NCloud::NProto::AUTHORIZATION_ACCEPT,
                overriddenConfig->GetAuthorizationMode());

            NKikimr::TControlBoard controlBoard;
            overriddenConfig->Register(controlBoard);

            TAtomic prevValue{};

            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_MaxMigrationBandwidth",
                100,
                prevValue));
            UNIT_ASSERT_VALUES_EQUAL(
                100,
                overriddenConfig->GetMaxMigrationBandwidth());

            UNIT_ASSERT(controlBoard.SetValue(
                "BlockStore_MaxMigrationBandwidth",
                0,
                prevValue));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                overriddenConfig->GetMaxMigrationBandwidth());

            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_DefaultTabletVersion",
                1,
                prevValue));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                overriddenConfig->GetDefaultTabletVersion());

            UNIT_ASSERT(!controlBoard.SetValue(
                "BlockStore_AuthorizationMode",
                2,
                prevValue));
            UNIT_ASSERT_EQUAL(
                NCloud::NProto::AUTHORIZATION_REQUIRE,
                overriddenConfig->GetAuthorizationMode());
        }

        // Check for RO overrides.
        {
            NProto::TStorageServiceConfig overriddenProto = []
            {
                NProto::TStorageServiceConfig proto;
                proto.SetSchemeShardDir("foo");
                proto.SetServiceVersionInfo("bar");
                return proto;
            }();
            const auto overriddenConfig = std::make_shared<TStorageConfig>(
                std::move(overriddenProto),
                std::make_shared<NFeatures::TFeaturesConfig>());

            UNIT_ASSERT_VALUES_EQUAL(
                "foo",
                overriddenConfig->GetSchemeShardDir());
            UNIT_ASSERT_VALUES_EQUAL(
                "bar",
                overriddenConfig->GetServiceVersionInfo());
            UNIT_ASSERT_VALUES_EQUAL("", overriddenConfig->GetFolderId());
        }
    }

    Y_UNIT_TEST(ShouldOverrideDoublesViaImmediateControlBoard)
    {
        NProto::TStorageServiceConfig overriddenProto = []
        {
            NProto::TStorageServiceConfig proto;
            proto.SetNonReplicatedAgentTimeoutGrowthFactor(2.5);
            return proto;
        }();
        const auto overriddenConfig = std::make_shared<TStorageConfig>(
            std::move(overriddenProto),
            std::make_shared<NFeatures::TFeaturesConfig>());

        UNIT_ASSERT_VALUES_EQUAL(
            2.5,
            overriddenConfig->GetNonReplicatedAgentTimeoutGrowthFactor());
        UNIT_ASSERT_VALUES_EQUAL(
            50,
            overriddenConfig->GetDiskRegistryInitialAgentRejectionThreshold());

        NKikimr::TControlBoard controlBoard;
        overriddenConfig->Register(controlBoard);

        UNIT_ASSERT_VALUES_EQUAL(
            2.5,
            overriddenConfig->GetNonReplicatedAgentTimeoutGrowthFactor());
        UNIT_ASSERT_VALUES_EQUAL(
            50,
            overriddenConfig->GetDiskRegistryInitialAgentRejectionThreshold());

        TAtomic prevValue{};
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_NonReplicatedAgentTimeoutGrowthFactor",
            123,
            prevValue));
        UNIT_ASSERT_VALUES_EQUAL(
            123,
            overriddenConfig->GetNonReplicatedAgentTimeoutGrowthFactor());

        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_DiskRegistryInitialAgentRejectionThreshold",
            456,
            prevValue));
        UNIT_ASSERT_VALUES_EQUAL(
            456,
            overriddenConfig->GetDiskRegistryInitialAgentRejectionThreshold());
    }

    Y_UNIT_TEST(ShouldOverrideNegativeValuesViaImmediateControlBoard)
    {
        NProto::TStorageServiceConfig overriddenProto = []
        {
            NProto::TStorageServiceConfig proto;
            proto.SetNonReplicatedAgentTimeoutGrowthFactor(-2.5);
            return proto;
        }();
        const auto overriddenConfig = std::make_shared<TStorageConfig>(
            std::move(overriddenProto),
            std::make_shared<NFeatures::TFeaturesConfig>());

        UNIT_ASSERT_VALUES_EQUAL(
            -2.5,
            overriddenConfig->GetNonReplicatedAgentTimeoutGrowthFactor());
        UNIT_ASSERT_VALUES_EQUAL(
            50,
            overriddenConfig->GetDiskRegistryInitialAgentRejectionThreshold());

        NKikimr::TControlBoard controlBoard;
        overriddenConfig->Register(controlBoard);

        UNIT_ASSERT_VALUES_EQUAL(
            -2.5,
            overriddenConfig->GetNonReplicatedAgentTimeoutGrowthFactor());
        UNIT_ASSERT_VALUES_EQUAL(
            50,
            overriddenConfig->GetDiskRegistryInitialAgentRejectionThreshold());

        TAtomic prevValue{};
        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_NonReplicatedAgentTimeoutGrowthFactor",
            -123,
            prevValue));
        UNIT_ASSERT_VALUES_EQUAL(
            -123,
            overriddenConfig->GetNonReplicatedAgentTimeoutGrowthFactor());

        UNIT_ASSERT(!controlBoard.SetValue(
            "BlockStore_DiskRegistryInitialAgentRejectionThreshold",
            -456,
            prevValue));
        UNIT_ASSERT_VALUES_EQUAL(
            -456,
            overriddenConfig->GetDiskRegistryInitialAgentRejectionThreshold());
    }

    Y_UNIT_TEST(ShouldAdaptNodeRegistrationParams)
    {
        NProto::TServerConfig serverConfig;
        serverConfig.SetNodeRegistrationMaxAttempts(10);
        serverConfig.SetNodeRegistrationErrorTimeout(20);
        serverConfig.SetVhostDiscardEnabled(true);

        NProto::TStorageServiceConfig storageConfigProto = []
        {
            NProto::TStorageServiceConfig proto;
            proto.SetNodeRegistrationMaxAttempts(30);
            proto.SetNodeRegistrationTimeout(40);
            return proto;
        }();

        AdaptNodeRegistrationParams("foobar", serverConfig, storageConfigProto);

        const auto storageConfig = std::make_shared<TStorageConfig>(
            std::move(storageConfigProto),
            std::make_shared<NFeatures::TFeaturesConfig>());

        UNIT_ASSERT_VALUES_EQUAL(
            30,
            storageConfig->GetNodeRegistrationMaxAttempts());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(20),
            storageConfig->GetNodeRegistrationErrorTimeout());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(40),
            storageConfig->GetNodeRegistrationTimeout());
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            storageConfig->GetEnableVhostDiscardForNewVolumes());
        UNIT_ASSERT_VALUES_EQUAL(
            "root@builtin",
            storageConfig->GetNodeRegistrationToken());
        UNIT_ASSERT_VALUES_EQUAL("foobar", storageConfig->GetNodeType());
    }

    Y_UNIT_TEST(ShouldAdaptNodeRegistrationParamsWhileZeroOverridden)
    {
        NProto::TServerConfig serverConfig;
        serverConfig.SetNodeRegistrationMaxAttempts(10);

        NProto::TStorageServiceConfig storageConfigProto = []
        {
            NProto::TStorageServiceConfig proto;
            proto.SetNodeRegistrationMaxAttempts(0);
            return proto;
        }();

        AdaptNodeRegistrationParams("", serverConfig, storageConfigProto);

        const auto storageConfig = std::make_shared<TStorageConfig>(
            std::move(storageConfigProto),
            std::make_shared<NFeatures::TFeaturesConfig>());

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            storageConfig->GetNodeRegistrationMaxAttempts());
        UNIT_ASSERT_VALUES_EQUAL("", storageConfig->GetNodeType());
    }

    Y_UNIT_TEST(ShouldCalcLinkedDisksBandwidthWithoutConfig)
    {
        using EStorageMediaKind = NCloud::NProto::EStorageMediaKind;
        NProto::TStorageServiceConfig globalConfigProto;
        auto globalConfig = std::make_shared<TStorageConfig>(
            globalConfigProto,
            std::make_shared<NFeatures::TFeaturesConfig>());
        auto ssdToSsd = GetLinkedDiskFillBandwidth(
            *globalConfig,
            EStorageMediaKind::STORAGE_MEDIA_SSD,
            EStorageMediaKind::STORAGE_MEDIA_SSD);

        UNIT_ASSERT_VALUES_EQUAL(100, ssdToSsd.Bandwidth);
        UNIT_ASSERT_VALUES_EQUAL(1, ssdToSsd.IoDepth);
    }

    Y_UNIT_TEST(ShouldCalcLinkedDisksBandwidthWithDefault)
    {
        using EStorageMediaKind = NCloud::NProto::EStorageMediaKind;
        NProto::TStorageServiceConfig globalConfigProto;
        {
            NProto::TLinkedDiskFillBandwidth defaultBandwidth;
            defaultBandwidth.SetReadBandwidth(200);
            defaultBandwidth.SetReadIoDepth(2);
            defaultBandwidth.SetWriteBandwidth(300);
            defaultBandwidth.SetWriteIoDepth(3);
            globalConfigProto.MutableLinkedDiskFillBandwidth()->Add(
                std::move(defaultBandwidth));
        }

        auto globalConfig = std::make_shared<TStorageConfig>(
            globalConfigProto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        auto ssdToSsd = GetLinkedDiskFillBandwidth(
            *globalConfig,
            EStorageMediaKind::STORAGE_MEDIA_SSD,
            EStorageMediaKind::STORAGE_MEDIA_SSD);
        UNIT_ASSERT_VALUES_EQUAL(200, ssdToSsd.Bandwidth);
        UNIT_ASSERT_VALUES_EQUAL(2, ssdToSsd.IoDepth);

        auto ssdToHdd = GetLinkedDiskFillBandwidth(
            *globalConfig,
            EStorageMediaKind::STORAGE_MEDIA_SSD,
            EStorageMediaKind::STORAGE_MEDIA_HDD);
        UNIT_ASSERT_VALUES_EQUAL(200, ssdToHdd.Bandwidth);
        UNIT_ASSERT_VALUES_EQUAL(2, ssdToHdd.IoDepth);
    }

    Y_UNIT_TEST(ShouldCalcLinkedDisksBandwidth)
    {
        using EStorageMediaKind = NCloud::NProto::EStorageMediaKind;
        NProto::TStorageServiceConfig globalConfigProto;
        {
            NProto::TLinkedDiskFillBandwidth defaultBandwidth;
            defaultBandwidth.SetReadBandwidth(150);
            defaultBandwidth.SetReadIoDepth(2);
            defaultBandwidth.SetWriteBandwidth(200);
            defaultBandwidth.SetWriteIoDepth(2);
            globalConfigProto.MutableLinkedDiskFillBandwidth()->Add(
                std::move(defaultBandwidth));
        }
        {
            NProto::TLinkedDiskFillBandwidth ssdBandwidth;
            ssdBandwidth.SetMediaKind(EStorageMediaKind::STORAGE_MEDIA_SSD);
            ssdBandwidth.SetReadBandwidth(300);
            ssdBandwidth.SetReadIoDepth(3);
            ssdBandwidth.SetWriteBandwidth(300);
            ssdBandwidth.SetWriteIoDepth(2);
            globalConfigProto.MutableLinkedDiskFillBandwidth()->Add(
                std::move(ssdBandwidth));
        }
        {
            NProto::TLinkedDiskFillBandwidth nrdBandwidth;
            nrdBandwidth.SetMediaKind(
                EStorageMediaKind::STORAGE_MEDIA_SSD_NONREPLICATED);
            nrdBandwidth.SetReadBandwidth(500);
            nrdBandwidth.SetReadIoDepth(4);
            nrdBandwidth.SetWriteBandwidth(400);
            nrdBandwidth.SetWriteIoDepth(4);
            globalConfigProto.MutableLinkedDiskFillBandwidth()->Add(
                std::move(nrdBandwidth));
        }

        auto globalConfig = std::make_shared<TStorageConfig>(
            globalConfigProto,
            std::make_shared<NFeatures::TFeaturesConfig>());

        {
            auto bandwidth = GetLinkedDiskFillBandwidth(
                *globalConfig,
                EStorageMediaKind::STORAGE_MEDIA_SSD,
                EStorageMediaKind::STORAGE_MEDIA_SSD);
            UNIT_ASSERT_VALUES_EQUAL(300, bandwidth.Bandwidth);
            UNIT_ASSERT_VALUES_EQUAL(2, bandwidth.IoDepth);
        }

        {
            auto bandwidth = GetLinkedDiskFillBandwidth(
                *globalConfig,
                EStorageMediaKind::STORAGE_MEDIA_SSD,
                EStorageMediaKind::STORAGE_MEDIA_SSD_NONREPLICATED);
            UNIT_ASSERT_VALUES_EQUAL(300, bandwidth.Bandwidth);
            UNIT_ASSERT_VALUES_EQUAL(3, bandwidth.IoDepth);
        }

        {
            auto bandwidth = GetLinkedDiskFillBandwidth(
                *globalConfig,
                EStorageMediaKind::STORAGE_MEDIA_SSD_NONREPLICATED,
                EStorageMediaKind::STORAGE_MEDIA_HDD);
            UNIT_ASSERT_VALUES_EQUAL(200, bandwidth.Bandwidth);
            UNIT_ASSERT_VALUES_EQUAL(2, bandwidth.IoDepth);
        }
        {
            auto bandwidth = GetLinkedDiskFillBandwidth(
                *globalConfig,
                EStorageMediaKind::STORAGE_MEDIA_HDD,
                EStorageMediaKind::STORAGE_MEDIA_SSD_NONREPLICATED);
            UNIT_ASSERT_VALUES_EQUAL(150, bandwidth.Bandwidth);
            UNIT_ASSERT_VALUES_EQUAL(2, bandwidth.IoDepth);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
