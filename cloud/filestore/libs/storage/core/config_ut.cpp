#include "config.h"

#include <cloud/filestore/config/storage.pb.h>

#include <cloud/storage/core/libs/features/features_config.h>

#include <contrib/ydb/core/protos/filestore_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TFeatureConfig& AddWhitelistedFeature(
    NCloud::NProto::TFeaturesConfig& featuresConfig,
    const TString& name,
    const TString& cloudName)
{
    auto* feature = featuresConfig.AddFeatures();
    feature->SetName(name);
    feature->MutableWhitelist()->AddCloudIds(cloudName);

    return *feature;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageConfigTest)
{
    Y_UNIT_TEST(ShouldCorrectlyGetFieldsByNamedGetters)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetPipeClientMinRetryTime(
            TDuration::Seconds(777).MilliSeconds());
        storageConfig.SetPipeClientMaxRetryTime(
            TDuration::Seconds(888).MilliSeconds());

        TStorageConfig config(storageConfig);

        // default values
        UNIT_ASSERT_VALUES_EQUAL("/Root", config.GetSchemeShardDir());
        UNIT_ASSERT_VALUES_EQUAL(4, config.GetPipeClientRetryCount());
        UNIT_ASSERT_VALUES_EQUAL(false, config.GetNewCleanupEnabled());

        // overridden values
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(777),
            config.GetPipeClientMinRetryTime());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(888),
            config.GetPipeClientMaxRetryTime());

        // feature-overridden values
        NCloud::NProto::TFeaturesConfig featuresConfigProto;
        featuresConfigProto.AddFeatures();
        featuresConfigProto.MutableFeatures(0)->SetName("NewCleanupEnabled");
        featuresConfigProto.MutableFeatures(0)->MutableWhitelist()->AddCloudIds(
            "test-cloud");

        config.SetFeaturesConfig(
            NFeatures::TFeaturesConfig(featuresConfigProto));

        config.SetCloudFolderEntity("other-cloud", "folder", "entity");
        UNIT_ASSERT_VALUES_EQUAL(false, config.GetNewCleanupEnabled());

        config.SetCloudFolderEntity("test-cloud", "folder", "entity");
        UNIT_ASSERT_VALUES_EQUAL(true, config.GetNewCleanupEnabled());
    }

    Y_UNIT_TEST(ShouldOverrideFieldsUsingFeaturesConfig)
    {
        const TString cloudName = "test-cloud";
        NCloud::NProto::TFeaturesConfig featuresConfigProto;
        AddWhitelistedFeature(featuresConfigProto, "SchemeShardDir", cloudName)
            .SetValue("/CustomRoot");
        AddWhitelistedFeature(
            featuresConfigProto,
            "PipeClientRetryCount",
            cloudName)
            .SetValue("11");
        AddWhitelistedFeature(
            featuresConfigProto,
            "PipeClientMinRetryTime",
            cloudName)
            .SetValue("2500");
        AddWhitelistedFeature(
            featuresConfigProto,
            "TenantHiveTabletId",
            cloudName)
            .SetValue("1234567890123");
        AddWhitelistedFeature(
            featuresConfigProto,
            "NewCleanupEnabled",
            cloudName);
        AddWhitelistedFeature(
            featuresConfigProto,
            "AuthorizationMode",
            cloudName)
            .SetValue("AUTHORIZATION_REQUIRE");

        TStorageConfig config;
        config.SetFeaturesConfig(
            NFeatures::TFeaturesConfig(featuresConfigProto));

        NProto::TError error =
            config.SetCloudFolderEntity(cloudName, "folder", "entity");
        UNIT_ASSERT_C(!HasError(error), error.GetMessage());

        UNIT_ASSERT_VALUES_EQUAL("/CustomRoot", config.GetSchemeShardDir());
        UNIT_ASSERT_VALUES_EQUAL(11, config.GetPipeClientRetryCount());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(2500),
            config.GetPipeClientMinRetryTime());
        UNIT_ASSERT_VALUES_EQUAL(1234567890123, config.GetTenantHiveTabletId());
        UNIT_ASSERT_VALUES_EQUAL(true, config.GetNewCleanupEnabled());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NCloud::NProto::AUTHORIZATION_REQUIRE),
            static_cast<int>(config.GetAuthorizationMode()));
    }

    Y_UNIT_TEST(ShouldReturnErrorForInvalidFeatureFieldValue)
    {
        const TString cloudName = "test-cloud";
        NCloud::NProto::TFeaturesConfig featuresConfigProto;
        AddWhitelistedFeature(
            featuresConfigProto,
            "PipeClientRetryCount",
            cloudName)
            .SetValue("akjdalihjsd");

        TStorageConfig config;
        config.SetFeaturesConfig(
            NFeatures::TFeaturesConfig(featuresConfigProto));

        NProto::TError error =
            config.SetCloudFolderEntity(cloudName, "folder", "entity");

        UNIT_ASSERT(HasError(error));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
        UNIT_ASSERT_C(
            error.GetMessage().Contains("akjdalihjsd"),
            error.GetMessage());
        UNIT_ASSERT_C(
            error.GetMessage().Contains("PipeClientRetryCount"),
            error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(4, config.GetPipeClientRetryCount());
    }
}

}   // namespace NCloud::NFileStore::NStorage
