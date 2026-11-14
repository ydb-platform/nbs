#include <cloud/blockstore/libs/config/blockstore_config_management.h>
#include <cloud/blockstore/libs/config/sections_update/features.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/unknown_field_set.h>

#include <memory>

namespace NCloud::NBlockStore {

namespace {

// Add a named feature with a value and one allowed cloud for merge checks.
NCloud::NProto::TFeatureConfig* AddFeature(
    NProto::TBlockstoreConfig& config,
    const TString& name,
    const TString& value,
    const TString& cloudId)
{
    auto* feature = config.MutableFeatures()->AddFeatures();
    feature->SetName(name);
    feature->SetValue(value);
    feature->MutableWhitelist()->AddCloudIds(cloudId);
    return feature;
}

}   // namespace

Y_UNIT_TEST_SUITE(TFeaturesConfigSectionTest)
{
    // Merge Features with repeated names. Check that the first record in each
    // source is used, a dynamic record fully replaces the same-name static
    // record, and runtime lookups use only the resulting records.
    Y_UNIT_TEST(ShouldReplaceStaticFeaturesWithDynamicFeatures)
    {
        NProto::TBlockstoreConfig staticConfig;
        auto* staticA =
            AddFeature(staticConfig, "A", "static-A", "static-A-cloud");
        staticA->SetCloudProbability(1);
        AddFeature(staticConfig, "B", "static-B", "static-B-cloud");
        AddFeature(
            staticConfig,
            "B",
            "duplicate-static-B",
            "duplicate-static-B-cloud");

        NProto::TBlockstoreConfig dynamicConfig;
        AddFeature(dynamicConfig, "A", "dynamic-A", "dynamic-A-cloud");
        AddFeature(dynamicConfig, "C", "dynamic-C", "dynamic-C-cloud");
        AddFeature(
            dynamicConfig,
            "C",
            "duplicate-dynamic-C",
            "duplicate-dynamic-C-cloud");

        const auto result =
            MergeBlockstoreConfigSources(staticConfig, dynamicConfig);

        UNIT_ASSERT_VALUES_EQUAL(3, result.GetFeatures().FeaturesSize());

        const auto& mergedA = result.GetFeatures().GetFeatures(0);
        UNIT_ASSERT_VALUES_EQUAL("A", mergedA.GetName());
        UNIT_ASSERT_VALUES_EQUAL("dynamic-A", mergedA.GetValue());
        UNIT_ASSERT(!mergedA.HasCloudProbability());
        UNIT_ASSERT_VALUES_EQUAL(1, mergedA.GetWhitelist().CloudIdsSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "dynamic-A-cloud",
            mergedA.GetWhitelist().GetCloudIds(0));

        const auto& mergedB = result.GetFeatures().GetFeatures(1);
        UNIT_ASSERT_VALUES_EQUAL("B", mergedB.GetName());
        UNIT_ASSERT_VALUES_EQUAL("static-B", mergedB.GetValue());

        const auto& mergedC = result.GetFeatures().GetFeatures(2);
        UNIT_ASSERT_VALUES_EQUAL("C", mergedC.GetName());
        UNIT_ASSERT_VALUES_EQUAL("dynamic-C", mergedC.GetValue());

        const auto config = MakeStartupBlockstoreConfig(
            staticConfig,
            dynamicConfig,
            std::make_shared<NStorage::TStorageConfigControls>());
        const auto& runtimeFeatures = *config->GetFeaturesConfig();

        UNIT_ASSERT_VALUES_EQUAL(
            result.GetFeatures().SerializeAsString(),
            runtimeFeatures.GetConfigProto().SerializeAsString());

        UNIT_ASSERT(
            runtimeFeatures.IsFeatureEnabled("dynamic-A-cloud", {}, {}, "A"));
        UNIT_ASSERT_VALUES_EQUAL(
            "dynamic-A",
            runtimeFeatures.GetFeatureValue("dynamic-A-cloud", {}, {}, "A"));
        UNIT_ASSERT(
            !runtimeFeatures.IsFeatureEnabled("static-A-cloud", {}, {}, "A"));
        UNIT_ASSERT(
            !runtimeFeatures.IsFeatureEnabled("other-cloud", {}, {}, "A"));

        UNIT_ASSERT(
            runtimeFeatures.IsFeatureEnabled("static-B-cloud", {}, {}, "B"));
        UNIT_ASSERT(
            !runtimeFeatures
                 .IsFeatureEnabled("duplicate-static-B-cloud", {}, {}, "B"));

        UNIT_ASSERT(
            runtimeFeatures.IsFeatureEnabled("dynamic-C-cloud", {}, {}, "C"));
        UNIT_ASSERT(
            !runtimeFeatures
                 .IsFeatureEnabled("duplicate-dynamic-C-cloud", {}, {}, "C"));
    }

    // Verify that Features composition preserves unknown container fields,
    // unrelated aggregate values and both source messages.
    Y_UNIT_TEST(ShouldPreserveFieldsOutsideFeatureDefinitions)
    {
        // Merge sources with unknown feature fields and an unrelated scalar.
        NProto::TBlockstoreConfig staticConfig;
        AddFeature(staticConfig, "A", "static", "cloud");
        auto* features = staticConfig.MutableFeatures();
        features->GetReflection()->MutableUnknownFields(features)->AddVarint(
            100,
            1);
        NProto::TBlockstoreConfig dynamicConfig;
        AddFeature(dynamicConfig, "A", "dynamic", "cloud");
        dynamicConfig.MutableStorageService()->SetWriteBlobThreshold(300);
        features = dynamicConfig.MutableFeatures();
        features->GetReflection()->MutableUnknownFields(features)->AddVarint(
            101,
            2);
        const auto staticBytes = staticConfig.SerializeAsString();
        const auto dynamicBytes = dynamicConfig.SerializeAsString();
        auto result = staticConfig;
        result.MergeFrom(dynamicConfig);

        // Replace named definitions without clearing the preceding proto merge.
        GetFeaturesSectionUpdater().Merge(staticConfig, dynamicConfig, result);
        UNIT_ASSERT_VALUES_EQUAL(1, result.GetFeatures().FeaturesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "dynamic",
            result.GetFeatures().GetFeatures(0).GetValue());
        const auto& mergedFeatures = result.GetFeatures();
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            mergedFeatures.GetReflection()
                ->GetUnknownFields(mergedFeatures)
                .field_count());
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            result.GetStorageService().GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(staticBytes, staticConfig.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(
            dynamicBytes,
            dynamicConfig.SerializeAsString());
    }
}

}   // namespace NCloud::NBlockStore
