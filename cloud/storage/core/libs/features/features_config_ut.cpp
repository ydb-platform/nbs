#include "features_config.h"

#include <cloud/storage/core/config/features.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

namespace NCloud::NFeatures {

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<TString> RandomStrings(ui32 n)
{
    TVector<TString> r;
    for (ui32 i = 0; i < n; ++i) {
        TString s(20, 0);
        for (ui32 j = 0; j < s.size(); ++j) {
            s[j] = RandomNumber<ui8>();
        }
        r.push_back(s);
    }
    return r;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFeaturesConfigTest)
{
    Y_UNIT_TEST(ShouldMatchCloudsByProbability)
    {
        NProto::TFeaturesConfig fc;
        auto* f = fc.AddFeatures();
        f->SetName("some_feature");
        f->SetCloudProbability(0.2);
        *f->MutableWhitelist()->AddCloudIds() = "whitelisted_cloud";
        TFeaturesConfig config(fc);

        auto clouds = RandomStrings(10000);
        ui32 matches = 0;
        for (const auto& cloud: clouds) {
            matches += config.IsFeatureEnabled(cloud, {}, {}, f->GetName());
        }

        UNIT_ASSERT_C(1500 < matches && matches < 2500, TStringBuilder()
            << "match count: " << matches);

        UNIT_ASSERT(config.IsFeatureEnabled(
            "whitelisted_cloud",
            {},
            {},
            f->GetName()));
    }

    Y_UNIT_TEST(ShouldMatchFoldersByProbability)
    {
        NProto::TFeaturesConfig fc;
        auto* f = fc.AddFeatures();
        f->SetName("some_feature");
        f->SetFolderProbability(0.3);
        *f->MutableWhitelist()->AddFolderIds() = "whitelisted_folder";
        TFeaturesConfig config(fc);

        auto folders = RandomStrings(10000);
        ui32 matches = 0;
        for (const auto& folder: folders) {
            matches += config.IsFeatureEnabled({}, folder, {}, f->GetName());
        }

        UNIT_ASSERT_C(2500 < matches && matches < 3500, TStringBuilder()
            << "match count: " << matches);

        UNIT_ASSERT(config.IsFeatureEnabled(
            {},
            "whitelisted_folder",
            {},
            f->GetName()));
    }

    Y_UNIT_TEST(ShouldMatchId)
    {
        NProto::TFeaturesConfig fc;
        auto* f = fc.AddFeatures();
        f->SetName("some_feature");
        *f->MutableWhitelist()->AddEntityIds() = "whitelisted_id";
        TFeaturesConfig config(fc);

        // id doesn't match "whitelisted_id"
        UNIT_ASSERT(!config.IsFeatureEnabled(
            "cloud_id",
            {},
            {},
            f->GetName()));
        UNIT_ASSERT(!config.IsFeatureEnabled(
            {},
            "folder_id",
            {},
            f->GetName()));
        UNIT_ASSERT(!config.IsFeatureEnabled(
            {},
            {},
            "id",
            f->GetName()));
        UNIT_ASSERT(!config.IsFeatureEnabled(
            "cloud_id",
            "folder_id",
            {},
            f->GetName()));
        UNIT_ASSERT(!config.IsFeatureEnabled(
            "cloud_id",
            {},
            "id",
            f->GetName()));
        UNIT_ASSERT(!config.IsFeatureEnabled(
            {},
            "folder_id",
            "id",
            f->GetName()));
        UNIT_ASSERT(!config.IsFeatureEnabled(
            "cloud_id",
            "folder_id",
            "id",
            f->GetName()));

        // id matches "whitelisted_id"
        UNIT_ASSERT(config.IsFeatureEnabled(
            {},
            {},
            "whitelisted_id",
            f->GetName()));
        UNIT_ASSERT(config.IsFeatureEnabled(
            "cloud_id",
            {},
            "whitelisted_id",
            f->GetName()));
        UNIT_ASSERT(config.IsFeatureEnabled(
            {},
            "folder_id",
            "whitelisted_id",
            f->GetName()));
        UNIT_ASSERT(config.IsFeatureEnabled(
            "cloud_id",
            "folder_id",
            "whitelisted_id",
            f->GetName()));
    }

    Y_UNIT_TEST(ShouldSupportWhiteAndBlacklistSimultaneously)
    {
        NProto::TFeaturesConfig fc;
        auto* f = fc.AddFeatures();
        f->SetName("feature");

        *f->MutableWhitelist()->AddCloudIds() = "white";
        *f->MutableWhitelist()->AddCloudIds() = "gray";

        *f->MutableBlacklist()->AddCloudIds() = "black";
        *f->MutableBlacklist()->AddCloudIds() = "gray";

        TFeaturesConfig config(fc);

        UNIT_ASSERT(config.IsFeatureEnabled("white", {}, {}, f->GetName()));
        UNIT_ASSERT(!config.IsFeatureEnabled("black", {}, {}, f->GetName()));

        // blacklist takes precedence
        UNIT_ASSERT(!config.IsFeatureEnabled("gray", {}, {}, f->GetName()));
    }

    Y_UNIT_TEST(ShouldEnableByDefaultIfWhitelistIsEmpty)
    {
        NProto::TFeaturesConfig fc;
        auto* f = fc.AddFeatures();
        f->SetName("feature");
        *f->MutableBlacklist()->AddCloudIds() = "black";
        TFeaturesConfig config(fc);

        auto clouds = RandomStrings(1000);
        int matches = 0;
        for (const auto& cloud: clouds) {
            matches += config.IsFeatureEnabled(cloud, {}, {}, f->GetName());
        }

        UNIT_ASSERT_C(matches == 1000, TStringBuilder()
            << "match count: " << matches);
    }
}

}   // namespace NCloud::NFeatures
