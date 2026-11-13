#include "node_registration_helpers.h"

#include <contrib/ydb/core/protos/feature_flags.pb.h>
#include <contrib/ydb/core/protos/netclassifier.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

#include <optional>
#include <unordered_set>

namespace NCloud {

using namespace NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

// This function can only check that primitive type fields are set.
// "presence" does not work for repeated fields and function will fail.
// So you have to add such fields to skipFields (to guarantee that you know what
// you are doing) and check them outside of this function.

template <typename TMessage>
void CheckAllFieldsSet(
    const TMessage& msg,
    const std::unordered_set<TString>& skipFields)
{
    for (int i = 0; i < msg.GetDescriptor()->field_count(); ++i) {
        const auto* field = msg.GetDescriptor()->field(i);

        if (skipFields.contains(field->name())) {
            continue;
        }

        if (field->options().deprecated()) {
            continue;
        }

        UNIT_ASSERT_C(
            field->has_presence(),
            TStringBuilder()
                << "Field "
                << field->DebugString()
                << "does not track presence");

        if (field->label() != NProtoBuf::FieldDescriptor::LABEL_OPTIONAL) {
            UNIT_ASSERT_C(
                msg.GetMetadata().reflection->HasField(msg, field),
                TStringBuilder()
                    << "Unset field: "
                    << field->DebugString());
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNodeRegistrationHelpersTest)
{
    // Verify that PROTO mode preserves all regular sections regardless of
    // whether YAML is absent, disabled, or enabled.
    Y_UNIT_TEST(ShouldPreserveProtoCmsSelection)
    {
        // Give the regular config distinct values in multiple sections.
        NKikimrConfig::TAppConfig regular;
        regular.MutableLogConfig()->SetDefaultLevel(3);
        regular.MutableFeatureFlags()->SetEnableVPatch(true);
        regular.AddNamedConfigs()->SetName("Cloud.NBS.StorageServiceConfig");

        // Reject YAML overrides even when the document enables YAML selection.
        for (const std::optional<bool> enabled:
             {std::optional<bool>(), std::optional(false), std::optional(true)})
        {
            NKikimrConfig::TAppConfig yaml;
            if (enabled) {
                yaml.SetYamlConfigEnabled(*enabled);
                yaml.MutableLogConfig()->SetDefaultLevel(7);
                yaml.MutableFeatureFlags()->SetEnableVPatch(false);
            }

            const auto selected = SelectCmsAppConfig(regular, yaml, false);
            UNIT_ASSERT_VALUES_EQUAL(
                regular.SerializeAsString(),
                selected.SerializeAsString());
        }
    }

    // Verify that YAML supplies LogConfig and FeatureFlags, while PROTO
    // supplies NameserviceConfig, NetClassifierDistributableConfig and
    // NamedConfigs.
    Y_UNIT_TEST(ShouldSelectFullYamlWithUnmanagedCmsSections)
    {
        // Give PROTO and YAML conflicting LogConfig, NameserviceConfig and
        // NamedConfigs values.
        NKikimrConfig::TAppConfig regular;
        regular.MutableLogConfig()->SetDefaultLevel(3);
        regular.MutableFeatureFlags()->SetEnableVPatch(true);
        regular.MutableNameserviceConfig()->SetSuppressVersionCheck(true);
        regular.MutableNetClassifierDistributableConfig()
            ->SetLastUpdateTimestamp(17);
        regular.AddNamedConfigs()->SetName("Cloud.NBS.StorageServiceConfig");
        NKikimrConfig::TAppConfig yaml;
        yaml.SetYamlConfigEnabled(true);
        yaml.MutableLogConfig()->SetDefaultLevel(7);
        yaml.MutableNameserviceConfig()->SetSuppressVersionCheck(false);
        yaml.AddNamedConfigs()->SetName("yaml-only");

        // Check YAML values, the three sections copied from PROTO, and the
        // absence of FeatureFlags that YAML did not set.
        const auto selected = SelectCmsAppConfig(regular, yaml, true);
        UNIT_ASSERT_VALUES_EQUAL(7, selected.GetLogConfig().GetDefaultLevel());
        UNIT_ASSERT(!selected.HasFeatureFlags());
        UNIT_ASSERT(selected.GetNameserviceConfig().GetSuppressVersionCheck());
        UNIT_ASSERT_VALUES_EQUAL(
            regular.GetNetClassifierDistributableConfig().SerializeAsString(),
            selected.GetNetClassifierDistributableConfig().SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(1, selected.NamedConfigsSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "Cloud.NBS.StorageServiceConfig",
            selected.GetNamedConfigs(0).GetName());
    }

    // Verify that absent or disabled YAML selects the complete regular config.
    Y_UNIT_TEST(ShouldSelectRegularConfigWhenYamlIsDisabled)
    {
        // Set multiple regular sections to detect partial source replacement.
        NKikimrConfig::TAppConfig regular;
        regular.MutableLogConfig()->SetDefaultLevel(3);
        regular.MutableFeatureFlags()->SetEnableVPatch(true);

        // Check both missing YAML and an explicitly disabled YAML document.
        for (bool hasYaml: {false, true}) {
            NKikimrConfig::TAppConfig yaml;
            if (hasYaml) {
                yaml.SetYamlConfigEnabled(false);
                yaml.MutableLogConfig()->SetDefaultLevel(7);
                yaml.MutableFeatureFlags()->SetEnableVPatch(false);
            }

            const auto selected = SelectCmsAppConfig(regular, yaml, true);
            UNIT_ASSERT_VALUES_EQUAL(
                regular.SerializeAsString(),
                selected.SerializeAsString());
        }
    }

    Y_UNIT_TEST(ShouldFillNodeInfo)
    {
        NYdb::NDiscovery::TNodeInfo info;

        {
            auto msg = CreateNodeInfo(info, {});
            CheckAllFieldsSet(msg, {"Name"});
        }

        {
            auto msg = CreateNodeInfo(info, "xyz");
            CheckAllFieldsSet(msg, {});
        }
    }

    Y_UNIT_TEST(ShouldFillLocationInfo)
    {
        NYdb::NDiscovery::TNodeLocation location;
        location.DataCenter = "data center";
        location.Module = "module";
        location.Rack = "rack";
        location.Unit = "unit";
        auto msg = CreateNodeLocation(location);
        CheckAllFieldsSet(msg, {});
    }

    Y_UNIT_TEST(ShouldFillStaticNodeInfo)
    {
        {
            NYdb::NDiscovery::TNodeInfo info;
            auto msg = CreateStaticNodeInfo(info);
            CheckAllFieldsSet(msg, {"Endpoint"});
        }

        {
            NKikimrNodeBroker::TNodeInfo info;
            auto msg = CreateStaticNodeInfo(info);
            CheckAllFieldsSet(msg, {"Endpoint"});
        }
    }
}

}   // namespace NCloud
