#include "config.h"

#include <cloud/filestore/config/storage.pb.h>

#include <cloud/storage/core/libs/features/features_config.h>

#include <contrib/ydb/core/protos/filestore_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

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
            std::make_shared<NFeatures::TFeaturesConfig>(featuresConfigProto));

        config.SetCloudFolderEntity("other-cloud", "folder", "entity");
        UNIT_ASSERT_VALUES_EQUAL(false, config.GetNewCleanupEnabled());

        config.SetCloudFolderEntity("test-cloud", "folder", "entity");
        UNIT_ASSERT_VALUES_EQUAL(true, config.GetNewCleanupEnabled());
    }
}

}   // namespace NCloud::NFileStore::NStorage
