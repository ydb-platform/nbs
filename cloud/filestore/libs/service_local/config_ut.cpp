#include "config.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TProtoSetter = void (NProto::TLocalServiceConfig::*)(bool);
using TPlainGetter = bool (TLocalFileStoreConfig::*)() const;
using TFeatureGetter = bool (TLocalFileStoreConfig::*)(
    const TString&,
    const TString&,
    const TString&) const;

struct TBinaryFeatureCase
{
    TString Name;
    TProtoSetter ProtoSetter;
    TPlainGetter PlainGetter;
    TFeatureGetter FeatureGetter;
};

void ShouldUseBinaryFeatureFromProtoConfig(const TBinaryFeatureCase& test)
{
    NProto::TLocalServiceConfig proto;
    (proto.*test.ProtoSetter)(true);

    TLocalFileStoreConfig config(proto);

    UNIT_ASSERT((config.*test.PlainGetter)());
}

void ShouldOverrideBinaryFeatureUsingFeaturesConfig(
    const TBinaryFeatureCase& test)
{
    const TString fsId = "fs-id";
    const TString anotherFsId = "another-fs-id";

    // no feature match, use proto config
    {
        NProto::TLocalServiceConfig proto;
        (proto.*test.ProtoSetter)(false);

        TLocalFileStoreConfig config(proto);

        NCloud::NProto::TFeaturesConfig featuresProto;
        auto* feature = featuresProto.AddFeatures();
        feature->SetName(test.Name);
        *feature->MutableWhitelist()->AddEntityIds() = anotherFsId;

        config.SetFeaturesConfig(
            std::make_shared<NFeatures::TFeaturesConfig>(featuresProto));

        UNIT_ASSERT(!(config.*test.FeatureGetter)({}, {}, fsId));
    }

    // feature enabled in whitelist without explicit value
    {
        NProto::TLocalServiceConfig proto;
        (proto.*test.ProtoSetter)(false);

        TLocalFileStoreConfig config(proto);

        NCloud::NProto::TFeaturesConfig featuresProto;
        auto* feature = featuresProto.AddFeatures();
        feature->SetName(test.Name);
        *feature->MutableWhitelist()->AddEntityIds() = fsId;

        config.SetFeaturesConfig(
            std::make_shared<NFeatures::TFeaturesConfig>(featuresProto));

        UNIT_ASSERT((config.*test.FeatureGetter)({}, {}, fsId));
    }

    // feature enabled in whitelist with true value
    {
        NProto::TLocalServiceConfig proto;
        (proto.*test.ProtoSetter)(false);

        TLocalFileStoreConfig config(proto);

        NCloud::NProto::TFeaturesConfig featuresProto;
        auto* feature = featuresProto.AddFeatures();
        feature->SetName(test.Name);
        feature->SetValue("true");
        *feature->MutableWhitelist()->AddEntityIds() = fsId;

        config.SetFeaturesConfig(
            std::make_shared<NFeatures::TFeaturesConfig>(featuresProto));

        UNIT_ASSERT((config.*test.FeatureGetter)({}, {}, fsId));
    }

    // feature disabled in whitelist with false value
    {
        NProto::TLocalServiceConfig proto;
        (proto.*test.ProtoSetter)(true);

        TLocalFileStoreConfig config(proto);

        NCloud::NProto::TFeaturesConfig featuresProto;
        auto* feature = featuresProto.AddFeatures();
        feature->SetName(test.Name);
        feature->SetValue("false");
        *feature->MutableWhitelist()->AddEntityIds() = fsId;

        config.SetFeaturesConfig(
            std::make_shared<NFeatures::TFeaturesConfig>(featuresProto));

        UNIT_ASSERT(!(config.*test.FeatureGetter)({}, {}, fsId));
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConfigTest)
{
    Y_UNIT_TEST(ShouldUseBinaryFeaturesFromProtoConfig)
    {
        const TBinaryFeatureCase tests[] = {
            {
                "ExtendedAttributesDisabled",
                &NProto::TLocalServiceConfig::SetExtendedAttributesDisabled,
                &TLocalFileStoreConfig::GetExtendedAttributesDisabled,
                &TLocalFileStoreConfig::GetExtendedAttributesDisabled,
            },
            {
                "SnapshotsDirEnabled",
                &NProto::TLocalServiceConfig::SetSnapshotsDirEnabled,
                &TLocalFileStoreConfig::GetSnapshotsDirEnabled,
                &TLocalFileStoreConfig::GetSnapshotsDirEnabled,
            },
            {
                "GuestHandleKillPrivV2Enabled",
                &NProto::TLocalServiceConfig::SetGuestHandleKillPrivV2Enabled,
                &TLocalFileStoreConfig::GetGuestHandleKillPrivV2Enabled,
                &TLocalFileStoreConfig::GetGuestHandleKillPrivV2Enabled,
            },
            {
                "GuestPosixAclEnabled",
                &NProto::TLocalServiceConfig::SetGuestPosixAclEnabled,
                &TLocalFileStoreConfig::GetGuestPosixAclEnabled,
                &TLocalFileStoreConfig::GetGuestPosixAclEnabled,
            },
        };

        for (const auto& test: tests) {
            ShouldUseBinaryFeatureFromProtoConfig(test);
        }
    }

    Y_UNIT_TEST(ShouldOverrideBinaryFeaturesUsingFeaturesConfig)
    {
        const TBinaryFeatureCase tests[] = {
            {
                "ExtendedAttributesDisabled",
                &NProto::TLocalServiceConfig::SetExtendedAttributesDisabled,
                &TLocalFileStoreConfig::GetExtendedAttributesDisabled,
                &TLocalFileStoreConfig::GetExtendedAttributesDisabled,
            },
            {
                "SnapshotsDirEnabled",
                &NProto::TLocalServiceConfig::SetSnapshotsDirEnabled,
                &TLocalFileStoreConfig::GetSnapshotsDirEnabled,
                &TLocalFileStoreConfig::GetSnapshotsDirEnabled,
            },
            {
                "GuestHandleKillPrivV2Enabled",
                &NProto::TLocalServiceConfig::SetGuestHandleKillPrivV2Enabled,
                &TLocalFileStoreConfig::GetGuestHandleKillPrivV2Enabled,
                &TLocalFileStoreConfig::GetGuestHandleKillPrivV2Enabled,
            },
            {
                "GuestPosixAclEnabled",
                &NProto::TLocalServiceConfig::SetGuestPosixAclEnabled,
                &TLocalFileStoreConfig::GetGuestPosixAclEnabled,
                &TLocalFileStoreConfig::GetGuestPosixAclEnabled,
            },
        };

        for (const auto& test: tests) {
            ShouldOverrideBinaryFeatureUsingFeaturesConfig(test);
        }
    }

    Y_UNIT_TEST(ShouldUseAioAsDefault)
    {
        TLocalFileStoreConfig config;

        const TFileIOConfig fileIO = config.GetFileIOConfig();
        UNIT_ASSERT(std::holds_alternative<TAioConfig>(fileIO));
    }

    Y_UNIT_TEST(ShouldConfigureTcMallocMetrics)
    {
        {
            TLocalFileStoreConfig config;

            UNIT_ASSERT(!config.GetEnableTcMallocMetrics());
        }

        {
            NProto::TLocalServiceConfig proto;
            proto.SetEnableTcMallocMetrics(true);

            TLocalFileStoreConfig config(proto);

            UNIT_ASSERT(config.GetEnableTcMallocMetrics());
        }
    }
}

}   // namespace NCloud::NFileStore
