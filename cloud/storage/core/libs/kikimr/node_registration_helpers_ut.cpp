#include "node_registration_helpers.h"

#include <contrib/ydb/core/protos/feature_flags.pb.h>
#include <contrib/ydb/core/protos/nbs/blockstore.pb.h>
#include <contrib/ydb/core/protos/netclassifier.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

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
    // Verify that PROTO mode takes only TAppConfig::BlockstoreConfig from YAML
    // regardless of YamlConfigEnabled, and clears it when YAML is absent.
    Y_UNIT_TEST(ShouldPreserveProtoCmsSelection)
    {
        // Give PROTO and YAML different LogConfig and
        // TAppConfig::BlockstoreConfig values.
        NKikimrConfig::TAppConfig regular;
        regular.MutableLogConfig()->SetDefaultLevel(3);
        regular.MutableBlockstoreConfig()->SetVolumePreemptionType(
            NKikimrConfig::PREEMPTION_MOVE_MOST_HEAVY);
        regular.AddNamedConfigs()->SetName("Cloud.NBS.StorageServiceConfig");
        NKikimrConfig::TAppConfig yaml;
        yaml.MutableLogConfig()->SetDefaultLevel(7);
        yaml.MutableBlockstoreConfig()->SetVolumePreemptionType(
            NKikimrConfig::PREEMPTION_MOVE_LEAST_HEAVY);

        // Preserve every regular section except the replaced
        // TAppConfig::BlockstoreConfig.
        const auto selected = SelectCmsAppConfig(regular, yaml, false);
        UNIT_ASSERT_VALUES_EQUAL(3, selected.GetLogConfig().GetDefaultLevel());
        UNIT_ASSERT_VALUES_EQUAL(1, selected.NamedConfigsSize());
        UNIT_ASSERT_EQUAL(
            NKikimrConfig::PREEMPTION_MOVE_LEAST_HEAVY,
            selected.GetBlockstoreConfig().GetVolumePreemptionType());

        // Clear TAppConfig::BlockstoreConfig values when CMS returns no YAML.
        const auto absent = SelectCmsAppConfig(regular, {}, false);
        UNIT_ASSERT(absent.HasBlockstoreConfig());
        UNIT_ASSERT(!absent.GetBlockstoreConfig().HasVolumePreemptionType());
        UNIT_ASSERT_VALUES_EQUAL(3, absent.GetLogConfig().GetDefaultLevel());
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
        // absence of FeatureFlags and TAppConfig::BlockstoreConfig that YAML
        // did not set.
        const auto selected = SelectCmsAppConfig(regular, yaml, true);
        UNIT_ASSERT_VALUES_EQUAL(7, selected.GetLogConfig().GetDefaultLevel());
        UNIT_ASSERT(!selected.HasFeatureFlags());
        UNIT_ASSERT(!selected.HasBlockstoreConfig());
        UNIT_ASSERT(selected.GetNameserviceConfig().GetSuppressVersionCheck());
        UNIT_ASSERT_VALUES_EQUAL(
            regular.GetNetClassifierDistributableConfig().SerializeAsString(),
            selected.GetNetClassifierDistributableConfig().SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(1, selected.NamedConfigsSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "Cloud.NBS.StorageServiceConfig",
            selected.GetNamedConfigs(0).GetName());
    }

    // Verify that absent or disabled YAML selects the complete regular config,
    // including TAppConfig::BlockstoreConfig, as in YDB.
    Y_UNIT_TEST(ShouldSelectRegularConfigWhenYamlIsDisabled)
    {
        // Set TAppConfig::BlockstoreConfig in PROTO to check that absent or
        // disabled YAML preserves it when full YAML selection is requested.
        NKikimrConfig::TAppConfig regular;
        regular.MutableLogConfig()->SetDefaultLevel(3);
        regular.MutableBlockstoreConfig()->SetVolumePreemptionType(
            NKikimrConfig::PREEMPTION_MOVE_MOST_HEAVY);

        // Check both missing YAML and an explicitly disabled YAML document.
        for (bool hasYaml: {false, true}) {
            NKikimrConfig::TAppConfig yaml;
            if (hasYaml) {
                yaml.SetYamlConfigEnabled(false);
                yaml.MutableLogConfig()->SetDefaultLevel(7);
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
