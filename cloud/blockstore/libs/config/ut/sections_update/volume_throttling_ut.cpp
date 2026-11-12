#include <cloud/blockstore/libs/config/blockstore_config_management.h>
#include <cloud/blockstore/libs/config/opaque_config_parser.h>
#include <cloud/blockstore/libs/config/sections_update/volume_throttling.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/unknown_field_set.h>

#include <utility>

namespace NCloud::NBlockStore {

Y_UNIT_TEST_SUITE(TVolumeThrottlingConfigSectionTest)
{
    // Verify that VolumeThrottling dynamic rules replace static rules with the
    // same selector and precede the remaining static rules.
    Y_UNIT_TEST(ShouldMergeVolumeThrottlingConfiguration)
    {
        // Create duplicate filter rules and a rule for a group of disks.
        NProto::TBlockstoreConfig staticConfig;
        auto* rule = staticConfig.MutableVolumeThrottling()->AddRules();
        auto* filter = rule->MutableFilter();
        for (const TString id: {"b", "a", "a"}) {
            filter->AddCloudIds(id);
        }
        filter->AddFolderIds("folder-b");
        filter->AddFolderIds("folder-a");
        filter->AddMediaKinds(NProto::STORAGE_MEDIA_SSD);
        filter->AddMediaKinds(NProto::STORAGE_MEDIA_HDD);
        filter->AddMediaKinds(NProto::STORAGE_MEDIA_SSD);
        rule->MutableCoefficients()->SetMaxReadBandwidth(0.5);
        rule->MutableCoefficients()->SetMaxWriteBandwidth(0.75);
        const auto originalFilter = *rule;
        rule = staticConfig.MutableVolumeThrottling()->AddRules();
        rule->MutableDisks()->AddDiskIds("disk-a");
        rule->MutableDisks()->AddDiskIds("disk-b");
        rule->MutableCoefficients()->SetMaxReadBandwidth(0.8);
        staticConfig.MutableVolumeThrottling()->AddRules()->CopyFrom(
            originalFilter);

        // Add a rule for one disk and a replacement filter with the same values
        // in a different order and without duplicates.
        NProto::TBlockstoreConfig dynamicConfig;
        rule = dynamicConfig.MutableVolumeThrottling()->AddRules();
        rule->MutableDisks()->AddDiskIds("disk-a");
        rule->MutableCoefficients()->SetMaxReadBandwidth(0.75);
        rule = dynamicConfig.MutableVolumeThrottling()->AddRules();
        filter = rule->MutableFilter();
        filter->AddCloudIds("a");
        filter->AddCloudIds("b");
        filter->AddFolderIds("folder-a");
        filter->AddFolderIds("folder-b");
        filter->AddMediaKinds(NProto::STORAGE_MEDIA_HDD);
        filter->AddMediaKinds(NProto::STORAGE_MEDIA_SSD);
        rule->MutableCoefficients()->SetMaxReadBandwidth(0.25);

        // Check that runtime updates put dynamic rules first and do not inherit
        // coefficients omitted from a replacement.
        const auto startup = MergeBlockstoreConfigSources(staticConfig, {});
        NProto::TBlockstoreConfig runtime;
        const auto diagnostics = PrepareRuntimeBlockstoreConfig(
            staticConfig,
            startup,
            dynamicConfig,
            runtime);
        UNIT_ASSERT(diagnostics.IgnoredPaths.empty());
        const auto& rules = runtime.GetVolumeThrottling().GetRules();
        UNIT_ASSERT_VALUES_EQUAL(3, rules.size());
        UNIT_ASSERT_VALUES_EQUAL("disk-a", rules[0].GetDisks().GetDiskIds(0));
        UNIT_ASSERT_VALUES_EQUAL(
            0.25,
            rules[1].GetCoefficients().GetMaxReadBandwidth());
        UNIT_ASSERT(!rules[1].GetCoefficients().HasMaxWriteBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(2, rules[2].GetDisks().DiskIdsSize());

        // Check that repeated updates do not duplicate rules and an empty
        // dynamic list restores the static rules.
        const auto repeated =
            MergeBlockstoreConfigSources(staticConfig, dynamicConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            runtime.GetVolumeThrottling().SerializeAsString(),
            repeated.GetVolumeThrottling().SerializeAsString());
        dynamicConfig.MutableVolumeThrottling()->ClearRules();
        PrepareRuntimeBlockstoreConfig(
            staticConfig,
            startup,
            dynamicConfig,
            runtime);
        UNIT_ASSERT_VALUES_EQUAL(
            staticConfig.GetVolumeThrottling().SerializeAsString(),
            runtime.GetVolumeThrottling().SerializeAsString());
    }

    // Verify that VolumeThrottling deletes exact static selectors only when
    // Coefficients is absent and keeps static rules for empty Coefficients.
    Y_UNIT_TEST(ShouldDeleteVolumeThrottlingRules)
    {
        // Create duplicate rules for a disk group and a rule for one disk in
        // it.
        NProto::TBlockstoreConfig staticConfig;
        auto* rule = staticConfig.MutableVolumeThrottling()->AddRules();
        rule->MutableDisks()->AddDiskIds("a");
        rule->MutableDisks()->AddDiskIds("b");
        rule->MutableCoefficients()->SetMaxReadBandwidth(0.5);
        const auto group = *rule;
        staticConfig.MutableVolumeThrottling()->AddRules()->CopyFrom(group);
        rule = staticConfig.MutableVolumeThrottling()->AddRules();
        rule->MutableDisks()->AddDiskIds("a");
        rule->MutableCoefficients()->SetMaxReadBandwidth(0.75);

        // Delete all copies of the group rule and leave the rule for one disk.
        NProto::TBlockstoreConfig dynamicConfig;
        rule = dynamicConfig.MutableVolumeThrottling()->AddRules();
        rule->MutableDisks()->AddDiskIds("b");
        rule->MutableDisks()->AddDiskIds("a");
        rule->MutableDisks()->AddDiskIds("a");
        dynamicConfig.MutableVolumeThrottling()
            ->AddRules()
            ->MutableDisks()
            ->AddDiskIds("unknown");
        const auto startup = MergeBlockstoreConfigSources(staticConfig, {});
        NProto::TBlockstoreConfig runtime;
        PrepareRuntimeBlockstoreConfig(
            staticConfig,
            startup,
            dynamicConfig,
            runtime);
        UNIT_ASSERT_VALUES_EQUAL(1, runtime.GetVolumeThrottling().RulesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            0.75,
            runtime.GetVolumeThrottling()
                .GetRules(0)
                .GetCoefficients()
                .GetMaxReadBandwidth());

        // Keep static rules when dynamic Coefficients is explicitly empty.
        for (auto& deletion:
             *dynamicConfig.MutableVolumeThrottling()->MutableRules())
        {
            deletion.MutableCoefficients();
        }
        const auto emptyDiagnostics = PrepareRuntimeBlockstoreConfig(
            staticConfig,
            startup,
            dynamicConfig,
            runtime);
        UNIT_ASSERT(emptyDiagnostics.IgnoredPaths.empty());
        UNIT_ASSERT_VALUES_EQUAL(
            staticConfig.GetVolumeThrottling().SerializeAsString(),
            runtime.GetVolumeThrottling().SerializeAsString());

        // Keep static rules when Coefficients contains only unknown fields.
        auto* coefficients = dynamicConfig.MutableVolumeThrottling()
                                 ->MutableRules(0)
                                 ->MutableCoefficients();
        coefficients->GetReflection()
            ->MutableUnknownFields(coefficients)
            ->AddVarint(100, 1);
        PrepareRuntimeBlockstoreConfig(
            staticConfig,
            startup,
            dynamicConfig,
            runtime);
        UNIT_ASSERT_VALUES_EQUAL(
            staticConfig.GetVolumeThrottling().SerializeAsString(),
            runtime.GetVolumeThrottling().SerializeAsString());

        // Delete the remaining rule, then remove dynamic config to restore
        // static.
        for (auto& deletion:
             *dynamicConfig.MutableVolumeThrottling()->MutableRules())
        {
            deletion.ClearCoefficients();
        }
        dynamicConfig.MutableVolumeThrottling()
            ->AddRules()
            ->MutableDisks()
            ->AddDiskIds("a");
        const auto diagnostics = PrepareRuntimeBlockstoreConfig(
            staticConfig,
            startup,
            dynamicConfig,
            runtime);
        UNIT_ASSERT(diagnostics.IgnoredPaths.empty());
        UNIT_ASSERT_VALUES_EQUAL(0, runtime.GetVolumeThrottling().RulesSize());
        PrepareRuntimeBlockstoreConfig(staticConfig, startup, {}, runtime);
        UNIT_ASSERT_VALUES_EQUAL(
            staticConfig.GetVolumeThrottling().SerializeAsString(),
            runtime.GetVolumeThrottling().SerializeAsString());
    }

    // Verify that VolumeThrottling applies known parameters, skips unsupported
    // rules and rejects invalid coefficients or duplicate active selectors.
    Y_UNIT_TEST(ShouldPrepareVolumeThrottlingConfiguration)
    {
        const auto parser = CreateBlockstoreOpaqueConfigParser();

        // Accept empty rule lists, deletion rules and explicit false or zero.
        const std::pair<TString, int> validConfigs[] = {
            {"volume_throttling: {rules: []}", 0},
            {"volume_throttling: {rules: [{filter: {}}]}", 0},
            {"volume_throttling: {rules: [{filter: {}, coefficients: {}}]}", 0},
            {"volume_throttling: {rules: [{filter: {}, coefficients: "
             "{throttling_enabled: false}}]}",
             1},
            {"volume_throttling: {rules: [{filter: {}, coefficients: "
             "{max_read_iops: 0}}]}",
             1},
            {"volume_throttling: {rules: [{}]}", 0},
            {"volume_throttling: {rules: [{future_selector: {}, "
             "coefficients: {max_read_iops: 2}}]}",
             0},
            {"volume_throttling: {rules: [{filter: {}, "
             "coefficients: {future_coefficient: 0.5}}]}",
             0},
            {"volume_throttling: {rules: [{filter: {}, coefficients: "
             "{future_coefficient: 0.5, max_read_iops: 0.25}}]}",
             1},
            {"volume_throttling: {rules: [{filter: {future_zone: zone}, "
             "coefficients: {max_read_iops: 0.25}}]}",
             1},
            {"volume_throttling: {rules: [{filter: {}, coefficients: {}}, "
             "{filter: {}, coefficients: {max_read_iops: 0.25}}]}",
             1},
        };
        for (const auto& [yaml, expectedRules]: validConfigs) {
            const auto message = parser(yaml);
            auto [config, error] = ExtractBlockstoreConfig(*message);
            UNIT_ASSERT_C(!HasError(error), yaml);
            TVector<TString> issues;
            UNIT_ASSERT_C(
                !HasError(PrepareDynamicVolumeThrottlingConfig(
                    *config.MutableVolumeThrottling(),
                    issues)),
                yaml);

            // Preserve explicit default values while omitting deletion rules.
            const auto merged = MergeBlockstoreConfigSources({}, config);
            UNIT_ASSERT_VALUES_EQUAL(
                expectedRules,
                merged.GetVolumeThrottling().RulesSize());
            if (expectedRules) {
                UNIT_ASSERT_VALUES_EQUAL(
                    config.GetVolumeThrottling().SerializeAsString(),
                    merged.GetVolumeThrottling().SerializeAsString());
            }
        }

        // Locate invalid coefficients and duplicate active selectors,
        // including deletions and explicit zero coefficients.
        const std::pair<TString, TString> invalidConfigs[] = {
            {"volume_throttling: {rules: [{filter: {}, coefficients: "
             "{max_read_iops: 2}}]}",
             "VolumeThrottling.Rules[0].Coefficients.MaxReadIops = 2"},
            {"volume_throttling: {rules: ["
             "{filter: {}, coefficients: {}}, "
             "{filter: {}, coefficients: {max_read_iops: 2}}]}",
             "VolumeThrottling.Rules[1].Coefficients.MaxReadIops = 2"},
            {"volume_throttling: {rules: ["
             "{disks: {disk_ids: [a, b]}}, "
             "{disks: {disk_ids: [b, a, a]}, "
             "coefficients: {max_read_iops: 0}}]}",
             "VolumeThrottling.Rules[1].Disks: same selector as "
             "VolumeThrottling.Rules[0].Disks"},
        };
        for (const auto& [yaml, expectedError]: invalidConfigs) {
            const auto message = parser(yaml);
            auto [config, error] = ExtractBlockstoreConfig(*message);
            UNIT_ASSERT_C(!HasError(error), yaml);
            TVector<TString> issues;
            const auto original =
                config.GetVolumeThrottling().SerializeAsString();
            const auto validationError = PrepareDynamicVolumeThrottlingConfig(
                *config.MutableVolumeThrottling(),
                issues);
            UNIT_ASSERT_C(HasError(validationError), yaml);
            UNIT_ASSERT_STRING_CONTAINS(
                validationError.GetMessage(),
                expectedError);
            UNIT_ASSERT_VALUES_EQUAL(
                original,
                config.GetVolumeThrottling().SerializeAsString());
        }

        // Reject invalid coefficients in typed updates as well as YAML.
        NProto::TBlockstoreConfig invalid;
        auto* rule = invalid.MutableVolumeThrottling()->AddRules();
        rule->MutableFilter();
        rule->MutableCoefficients()->SetMaxReadIops(2.0);
        TVector<TString> issues;
        UNIT_ASSERT(HasError(PrepareDynamicVolumeThrottlingConfig(
            *invalid.MutableVolumeThrottling(),
            issues)));

        // Skip unknown-only coefficients instead of turning them into deletes.
        auto* coefficients = rule->MutableCoefficients();
        coefficients->Clear();
        coefficients->GetReflection()
            ->MutableUnknownFields(coefficients)
            ->AddVarint(100, 1);
        const auto unknownError = PrepareDynamicVolumeThrottlingConfig(
            *invalid.MutableVolumeThrottling(),
            issues);
        UNIT_ASSERT(!HasError(unknownError));
        UNIT_ASSERT_VALUES_EQUAL(0, invalid.GetVolumeThrottling().RulesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, issues.size());
        UNIT_ASSERT_STRING_CONTAINS(
            issues[0],
            "Skip VolumeThrottling.Rules[0]: "
            "Coefficients contains no known fields");
    }

    // Verify that VolumeThrottling issues use original rule indices and
    // dynamic preparation alone skips explicitly empty Coefficients.
    Y_UNIT_TEST(ShouldReportSkippedVolumeThrottlingRules)
    {
        // Distinguish an unsupported selector, an empty rule and explicit
        // false.
        NProto::TVolumeThrottlingServiceConfig config;
        config.AddRules()->MutableCoefficients()->SetMaxReadIops(2);
        auto* empty = config.AddRules();
        empty->MutableFilter();
        empty->MutableCoefficients();
        auto* rule = config.AddRules();
        rule->MutableFilter();
        rule->MutableCoefficients()->SetThrottlingEnabled(false);
        auto staticConfig = config;

        // Skip both unusable entries in dynamic and retain the false value.
        TVector<TString> issues;
        UNIT_ASSERT(
            !HasError(PrepareDynamicVolumeThrottlingConfig(config, issues)));
        UNIT_ASSERT_VALUES_EQUAL(2, issues.size());
        UNIT_ASSERT_STRING_CONTAINS(
            issues[0],
            "Skip VolumeThrottling.Rules[0]: no known selector (Filter or "
            "Disks)");
        UNIT_ASSERT_STRING_CONTAINS(issues[1], "VolumeThrottling.Rules[1]");
        UNIT_ASSERT_VALUES_EQUAL(1, config.RulesSize());
        UNIT_ASSERT(
            config.GetRules(0).GetCoefficients().HasThrottlingEnabled());

        // Preserve the existing static meaning of an explicitly empty rule.
        UNIT_ASSERT(!HasError(
            PrepareStaticVolumeThrottlingConfig(staticConfig, issues)));
        UNIT_ASSERT_VALUES_EQUAL(1, issues.size());
        UNIT_ASSERT_VALUES_EQUAL(2, staticConfig.RulesSize());
    }

    // Verify that a rejected VolumeThrottling section restores raw accepted
    // operations, or clears only that section when no accepted value exists.
    Y_UNIT_TEST(ShouldRestoreOnlyRejectedVolumeThrottlingSection)
    {
        // Include a deletion in the fallback to distinguish it from merged
        // rules.
        NProto::TBlockstoreConfig previous;
        previous.MutableVolumeThrottling()
            ->AddRules()
            ->MutableDisks()
            ->AddDiskIds("removed");
        const auto previousBytes = previous.SerializeAsString();
        const NProto::TBlockstoreConfig empty;
        const NProto::TBlockstoreConfig* previousConfigs[] = {
            nullptr,
            &empty,
            &previous};

        for (const auto* accepted: previousConfigs) {
            // Submit a skipped rule and an invalid rule beside another setting.
            NProto::TBlockstoreConfig candidate;
            candidate.MutableStorageService()->SetWriteBlobThreshold(300);
            candidate.MutableVolumeThrottling()->AddRules();
            auto* rule = candidate.MutableVolumeThrottling()->AddRules();
            rule->MutableFilter();
            rule->MutableCoefficients()->SetMaxReadIops(2);
            TVector<TString> issues;
            const auto error = GetVolumeThrottlingSectionUpdater().Prepare(
                candidate,
                accepted,
                issues);

            // Keep the diagnostics and unrelated value while replacing the
            // section.
            UNIT_ASSERT(HasError(error));
            UNIT_ASSERT_VALUES_EQUAL(1, issues.size());
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "VolumeThrottling.Rules[1].Coefficients.MaxReadIops = 2");
            UNIT_ASSERT_VALUES_EQUAL(
                300,
                candidate.GetStorageService().GetWriteBlobThreshold());
            UNIT_ASSERT_VALUES_EQUAL(
                accepted && accepted->HasVolumeThrottling(),
                candidate.HasVolumeThrottling());
            if (candidate.HasVolumeThrottling()) {
                UNIT_ASSERT_VALUES_EQUAL(
                    previous.GetVolumeThrottling().SerializeAsString(),
                    candidate.GetVolumeThrottling().SerializeAsString());
            }
        }

        // Leave the accepted dynamic source intact across every fallback path.
        UNIT_ASSERT_VALUES_EQUAL(previousBytes, previous.SerializeAsString());
    }
}

}   // namespace NCloud::NBlockStore
