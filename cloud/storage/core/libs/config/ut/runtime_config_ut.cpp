#include <cloud/storage/core/config/markers.pb.h>
#include <cloud/storage/core/libs/config/runtime_config.h>
#include <cloud/storage/core/libs/config/ut/protos/runtime_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/util/message_differencer.h>

#include <exception>

namespace NCloud::NConfig {

namespace {

// Add unknown data without changing any known field or its presence.
void AddUnknownField(google::protobuf::Message& message, ui64 value)
{
    message.GetReflection()->MutableUnknownFields(&message)->AddVarint(
        100,
        value);
}

}   // namespace

Y_UNIT_TEST_SUITE(TRuntimeConfigTest)
{
    // Verify the test schema, including ignored markers inside compound values.
    Y_UNIT_TEST(ShouldValidateRuntimeConfigSchema)
    {
        ValidateRuntimeConfigSchema(
            *NProto::NTest::TRuntimeConfig::descriptor());
    }

    // Verify that both descriptor mismatches throw before modifying runtime,
    // including unknown fields, and a subsequent valid call still succeeds.
    Y_UNIT_TEST(ShouldRejectMismatchedConfigTypes)
    {
        NProto::NTest::TRuntimeConfig staticConfig;
        NProto::NTest::TParameters otherConfig;
        auto runtimeConfig = staticConfig;
        AddUnknownField(runtimeConfig, 1);
        AddUnknownField(otherConfig, 2);
        const auto runtimeBefore = runtimeConfig.SerializeAsString();
        const auto otherBefore = otherConfig.SerializeAsString();

        // Identify the actual startup type without altering the runtime input.
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            FilterRuntimeConfig(staticConfig, otherConfig, runtimeConfig),
            std::exception,
            "Startup config type mismatch: expected static protobuf type "
            "'NCloud.NConfig.NProto.NTest.TRuntimeConfig', got startup type "
            "'NCloud.NConfig.NProto.NTest.TParameters'");
        UNIT_ASSERT_VALUES_EQUAL(
            runtimeBefore,
            runtimeConfig.SerializeAsString());

        // Identify the actual runtime type and preserve its original contents.
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            FilterRuntimeConfig(staticConfig, staticConfig, otherConfig),
            std::exception,
            "Runtime config type mismatch: expected static protobuf type "
            "'NCloud.NConfig.NProto.NTest.TRuntimeConfig', got runtime type "
            "'NCloud.NConfig.NProto.NTest.TParameters'");
        UNIT_ASSERT_VALUES_EQUAL(otherBefore, otherConfig.SerializeAsString());

        // Apply a valid update after both errors and remove its unknown data.
        runtimeConfig.SetFlag(true);
        const auto diagnostics =
            FilterRuntimeConfig(staticConfig, staticConfig, runtimeConfig);
        UNIT_ASSERT(runtimeConfig.GetFlag());
        UNIT_ASSERT(diagnostics.IgnoredPaths.empty());
        UNIT_ASSERT(runtimeConfig.GetReflection()
                        ->GetUnknownFields(runtimeConfig)
                        .empty());
    }

    // Verify all four ancestor/leaf permission combinations and restore
    // presence when an unmarked scalar is introduced at runtime.
    Y_UNIT_TEST(ShouldRequireMarkersOnTheWholePath)
    {
        // Give both open and closed containers distinguishable startup values.
        NProto::NTest::TRuntimeConfig startup;
        startup.MutableAllowed()->SetMutable(10);
        startup.MutableAllowed()->SetFrozen(20);
        startup.MutableFrozen()->SetMutable(30);
        startup.MutableExplicitFalse()->SetMutable(40);

        // Attempt every ancestor/leaf combination, including explicit false.
        auto requested = startup;
        requested.MutableAllowed()->SetMutable(11);
        requested.MutableAllowed()->SetFrozen(21);
        requested.MutableFrozen()->SetMutable(31);
        requested.MutableFrozen()->SetFrozen(0);
        requested.MutableExplicitFalse()->SetMutable(41);
        const auto diagnostics =
            FilterRuntimeConfig(startup, startup, requested);

        // Apply only the path whose container and parameter are both marked.
        UNIT_ASSERT_VALUES_EQUAL(11, requested.GetAllowed().GetMutable());
        UNIT_ASSERT_VALUES_EQUAL(20, requested.GetAllowed().GetFrozen());
        UNIT_ASSERT_VALUES_EQUAL(30, requested.GetFrozen().GetMutable());
        UNIT_ASSERT(!requested.GetFrozen().HasFrozen());
        UNIT_ASSERT_VALUES_EQUAL(40, requested.GetExplicitFalse().GetMutable());
        UNIT_ASSERT_VALUES_EQUAL(4, diagnostics.IgnoredPaths.size());
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("Allowed.Frozen") ==
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("Frozen.Mutable") ==
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("Frozen.Frozen") ==
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("ExplicitFalse.Mutable") ==
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
    }

    // Verify that collections are atomic parameters, preserve ordinary merge
    // semantics, and do not require markers on element or map entry fields.
    Y_UNIT_TEST(ShouldApplyCollectionsAsCompleteParameters)
    {
        // Prepare static and startup collections with different overrides.
        NProto::NTest::TRuntimeConfig staticConfig;
        staticConfig.AddValues(1);
        (*staticConfig.MutableMutableMap())["base"] = 1;
        auto startup = staticConfig;
        startup.AddFrozenValues("startup");
        (*startup.MutableFrozenMap())["private-key"] = 2;

        // Build runtime by merging collection overrides into static.
        NProto::NTest::TRuntimeConfig dynamicConfig;
        dynamicConfig.AddValues(2);
        dynamicConfig.AddRecords()->SetFrozen(42);
        dynamicConfig.AddFrozenValues("secret-value");
        (*dynamicConfig.MutableMutableMap())["new"] = 3;
        (*dynamicConfig.MutableFrozenMap())["secret-key"] = 4;
        auto requested = staticConfig;
        requested.MergeFrom(dynamicConfig);
        const auto diagnostics =
            FilterRuntimeConfig(staticConfig, startup, requested);

        // Keep complete allowed elements, restore frozen collections, and
        // report only collection schema paths without their keys or values.
        UNIT_ASSERT_VALUES_EQUAL(2, requested.ValuesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, requested.GetValues(0));
        UNIT_ASSERT_VALUES_EQUAL(2, requested.GetValues(1));
        UNIT_ASSERT_VALUES_EQUAL(42, requested.GetRecords(0).GetFrozen());
        UNIT_ASSERT_VALUES_EQUAL(2, requested.GetMutableMap().size());
        UNIT_ASSERT_VALUES_EQUAL("startup", requested.GetFrozenValues(0));
        UNIT_ASSERT_VALUES_EQUAL(1, requested.GetFrozenMap().size());
        UNIT_ASSERT_VALUES_EQUAL(2, requested.GetFrozenMap().at("private-key"));
        UNIT_ASSERT_VALUES_EQUAL(2, diagnostics.IgnoredPaths.size());
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("FrozenValues[]") ==
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("FrozenMap[]") ==
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);

        // Rebuild a replay and removal from static, without accumulating data.
        auto replay = staticConfig;
        replay.MergeFrom(dynamicConfig);
        FilterRuntimeConfig(staticConfig, startup, replay);
        UNIT_ASSERT(
            google::protobuf::util::MessageDifferencer::Equals(
                requested,
                replay));
        auto removed = staticConfig;
        FilterRuntimeConfig(staticConfig, startup, removed);
        UNIT_ASSERT_VALUES_EQUAL(1, removed.ValuesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, removed.RecordsSize());
        UNIT_ASSERT_VALUES_EQUAL("startup", removed.GetFrozenValues(0));
    }

    // Verify atomic oneof switches, restoration of the original frozen member,
    // and preservation of a frozen oneof that was unset at startup.
    Y_UNIT_TEST(ShouldPreserveOneofSelection)
    {
        // Give both choices a scalar startup member, including explicit zero.
        NProto::NTest::TRuntimeConfig startup;
        startup.SetMutableNumber(0);
        startup.SetFrozenNumber(0);
        auto requested = startup;
        requested.MutableMutableMessage()->SetValue(11);
        requested.MutableFrozenMessage()->SetValue(22);

        // Switch only the fully marked choice and preserve frozen presence.
        const auto diagnostics =
            FilterRuntimeConfig(startup, startup, requested);
        UNIT_ASSERT(requested.HasMutableMessage());
        UNIT_ASSERT_VALUES_EQUAL(11, requested.GetMutableMessage().GetValue());
        UNIT_ASSERT(requested.HasFrozenNumber());
        UNIT_ASSERT(!requested.HasFrozenMessage());
        UNIT_ASSERT_VALUES_EQUAL(0, requested.GetFrozenNumber());
        UNIT_ASSERT_VALUES_EQUAL(1, diagnostics.IgnoredPaths.size());
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("FrozenChoice") ==
            ERuntimeConfigIgnoreReason::OneofSwitchForbidden);

        // Clear the allowed choice on removal, but keep the frozen selection.
        requested.Clear();
        FilterRuntimeConfig(startup, startup, requested);
        UNIT_ASSERT(!requested.HasMutableNumber());
        UNIT_ASSERT(!requested.HasMutableMessage());
        UNIT_ASSERT(requested.HasFrozenNumber());

        // Ignore a new frozen member when the startup choice was unset.
        startup.ClearFrozenNumber();
        requested.MutableFrozenMessage()->SetValue(33);
        FilterRuntimeConfig(startup, startup, requested);
        UNIT_ASSERT(!requested.HasFrozenNumber());
        UNIT_ASSERT(!requested.HasFrozenMessage());
    }

    // Verify that rejecting a switch preserves the startup case while mutable
    // values fall back to the matching static branch.
    Y_UNIT_TEST(ShouldKeepStartupOneofCase)
    {
        // Give the startup override a different value from its static base.
        NProto::NTest::TRuntimeConfig staticConfig;
        staticConfig.MutableFixedMessage()->SetValue(10);
        auto startup = staticConfig;
        startup.MutableFixedMessage()->SetValue(20);

        // Attempt a switch that removes the static message during normal merge.
        auto requested = staticConfig;
        requested.SetFixedNumber(30);
        const auto diagnostics =
            FilterRuntimeConfig(staticConfig, startup, requested);

        // Recover the selected message from static, without retaining the
        // override.
        UNIT_ASSERT(requested.HasFixedMessage());
        UNIT_ASSERT_VALUES_EQUAL(10, requested.GetFixedMessage().GetValue());
        UNIT_ASSERT_VALUES_EQUAL(1, diagnostics.IgnoredPaths.size());
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("FixedChoice") ==
            ERuntimeConfigIgnoreReason::OneofSwitchForbidden);
    }

    // Verify that identical CMS sources allow updates only in the fixed oneof
    // branch selected by the effective startup configuration.
    Y_UNIT_TEST(ShouldUseStaticOneofSelectionWithEmptyStartupCms)
    {
        // Cover a matching static branch, a different branch, and no selection.
        TVector<NProto::NTest::TRuntimeConfig> staticConfigs(3);
        staticConfigs[0].MutableFixedMessage()->SetValue(10);
        staticConfigs[1].SetFixedNumber(7);
        const NProto::NTest::TRuntimeConfig startupCms;
        NProto::NTest::TRuntimeConfig runtimeCms;
        runtimeCms.MutableFixedMessage()->SetValue(20);

        for (const auto& staticConfig: staticConfigs) {
            // Resolve both complete configurations before checking permission.
            auto startup = staticConfig;
            startup.MergeFrom(startupCms);
            auto requested = staticConfig;
            requested.MergeFrom(runtimeCms);
            const auto diagnostics =
                FilterRuntimeConfig(staticConfig, startup, requested);

            // Update the matching branch and reject either kind of switch.
            if (staticConfig.HasFixedMessage()) {
                UNIT_ASSERT(requested.HasFixedMessage());
                UNIT_ASSERT_VALUES_EQUAL(
                    20,
                    requested.GetFixedMessage().GetValue());
                UNIT_ASSERT(diagnostics.IgnoredPaths.empty());
            } else {
                UNIT_ASSERT(
                    google::protobuf::util::MessageDifferencer::Equals(
                        staticConfig,
                        requested));
                UNIT_ASSERT_VALUES_EQUAL(1, diagnostics.IgnoredPaths.size());
                UNIT_ASSERT(
                    diagnostics.IgnoredPaths.at("FixedChoice") ==
                    ERuntimeConfigIgnoreReason::OneofSwitchForbidden);
            }
        }
    }

    // Verify that a frozen CMS override is diagnosed only when its effective
    // value differs from the startup value inherited from static.
    Y_UNIT_TEST(ShouldCompareFrozenOverridesAfterMergingStatic)
    {
        // Use one CMS override with both equal and different static defaults.
        NProto::NTest::TRuntimeConfig runtimeCms;
        runtimeCms.MutableAllowed()->SetFrozen(10);
        for (const ui32 staticValue: {10, 11}) {
            NProto::NTest::TRuntimeConfig staticConfig;
            staticConfig.MutableAllowed()->SetFrozen(staticValue);
            const auto startup = staticConfig;

            // Filter the merged request and preserve the complete startup.
            auto requested = staticConfig;
            requested.MergeFrom(runtimeCms);
            const auto diagnostics =
                FilterRuntimeConfig(staticConfig, startup, requested);
            UNIT_ASSERT(
                google::protobuf::util::MessageDifferencer::Equals(
                    startup,
                    requested));

            // Do not report a source change that leaves the effective value
            // unchanged; identify the rejected field for a real difference.
            UNIT_ASSERT_VALUES_EQUAL(
                staticValue != 10,
                diagnostics.IgnoredPaths.size());
            if (staticValue != 10) {
                UNIT_ASSERT(
                    diagnostics.IgnoredPaths.at("Allowed.Frozen") ==
                    ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
            }
        }
    }

    // Verify independent permissions inside a fixed message and different
    // permissions on other members of the same oneof.
    Y_UNIT_TEST(ShouldFilterFieldsInsideFixedOneof)
    {
        // Keep a mutable value and a startup-only sibling in the selected
        // member.
        NProto::NTest::TRuntimeConfig staticConfig;
        staticConfig.MutableFixedMessage()->SetValue(10);
        auto startup = staticConfig;
        startup.MutableFixedMessage()->SetValue(20);
        startup.MutableFixedMessage()->SetFrozen(30);
        auto requested = startup;
        requested.MutableFixedMessage()->SetValue(40);
        requested.MutableFixedMessage()->SetFrozen(50);

        // Apply only the marked child, without rejecting the mixed schema.
        const auto diagnostics =
            FilterRuntimeConfig(staticConfig, startup, requested);
        UNIT_ASSERT_VALUES_EQUAL(40, requested.GetFixedMessage().GetValue());
        UNIT_ASSERT_VALUES_EQUAL(30, requested.GetFixedMessage().GetFrozen());
        UNIT_ASSERT_VALUES_EQUAL(1, diagnostics.IgnoredPaths.size());
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("FixedMessage.Frozen") ==
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);

        // Remove the override while retaining the selected member and its
        // forbidden sibling field.
        requested = staticConfig;
        FilterRuntimeConfig(staticConfig, startup, requested);
        UNIT_ASSERT(requested.HasFixedMessage());
        UNIT_ASSERT_VALUES_EQUAL(10, requested.GetFixedMessage().GetValue());
        UNIT_ASSERT_VALUES_EQUAL(30, requested.GetFixedMessage().GetFrozen());

        // Select an unmarked member at another startup; its child marker cannot
        // bypass the missing permission on that member.
        startup.MutableReadOnlyMessage()->SetValue(60);
        requested = startup;
        requested.MutableReadOnlyMessage()->SetValue(70);
        FilterRuntimeConfig(staticConfig, startup, requested);
        UNIT_ASSERT_VALUES_EQUAL(60, requested.GetReadOnlyMessage().GetValue());
    }

    // Verify defaults rather than startup overrides when the fixed member has
    // no matching static branch, including scalar presence and startup unset.
    Y_UNIT_TEST(ShouldPreserveFixedOneofPresenceAndDefaults)
    {
        // Select a message in startup while leaving it absent from static.
        NProto::NTest::TRuntimeConfig staticConfig;
        staticConfig.SetFixedNumber(10);
        auto startup = staticConfig;
        startup.MutableFixedMessage()->SetValue(20);
        startup.MutableFixedMessage()->SetFrozen(30);

        // Use static as runtime; retain the startup selection and forbidden
        // value, and reset the permitted value to its default.
        auto requested = staticConfig;
        FilterRuntimeConfig(staticConfig, startup, requested);
        UNIT_ASSERT(requested.HasFixedMessage());
        UNIT_ASSERT(!requested.GetFixedMessage().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(30, requested.GetFixedMessage().GetFrozen());

        // Keep the startup message selected when static and runtime have no
        // selected member.
        staticConfig.Clear();
        startup.MutableFixedMessage()->Clear();
        requested.Clear();
        FilterRuntimeConfig(staticConfig, startup, requested);
        UNIT_ASSERT(requested.HasFixedMessage());
        UNIT_ASSERT_VALUES_EQUAL(0, requested.GetFixedMessage().ByteSizeLong());

        // Restore a selected scalar with its default, not its startup override.
        startup.SetFixedNumber(42);
        requested.MutableFixedMessage()->SetValue(99);
        FilterRuntimeConfig(staticConfig, startup, requested);
        UNIT_ASSERT(requested.HasFixedNumber());
        UNIT_ASSERT_VALUES_EQUAL(0, requested.GetFixedNumber());

        // Keep startup unset even when the new member itself permits updates.
        startup.Clear();
        requested.SetFixedNumber(7);
        FilterRuntimeConfig(staticConfig, startup, requested);
        UNIT_ASSERT(!requested.HasFixedNumber());
        UNIT_ASSERT(!requested.HasFixedMessage());
    }

    // Verify that a switchable message grants permission to its known contents,
    // including nested choices and annotations that are ignored in compound
    // values.
    Y_UNIT_TEST(ShouldUpdateCompoundOneofContents)
    {
        // Retain ordinary merge behavior for an unchanged message member.
        NProto::NTest::TRuntimeConfig staticConfig;
        staticConfig.MutableMutableMessage()->SetValue(10);
        staticConfig.MutableMutableMessage()->MutableNested()->SetMutable(11);
        auto startup = staticConfig;
        startup.MutableMutableMessage()->SetFirst(1);
        NProto::NTest::TRuntimeConfig dynamicConfig;
        dynamicConfig.MutableMutableMessage()->MutableNested()->SetFrozen(20);
        dynamicConfig.MutableMutableMessage()->SetSecond("new-case");
        dynamicConfig.MutableMutableMessage()->SetExplicitFalse(30);
        dynamicConfig.AddAtomicRecords()->SetUnmarked(40);
        auto requested = staticConfig;
        requested.MergeFrom(dynamicConfig);

        // Ignore inner markers and inner switch policies; known values follow
        // merge.
        const auto diagnostics =
            FilterRuntimeConfig(staticConfig, startup, requested);
        UNIT_ASSERT_VALUES_EQUAL(0, diagnostics.IgnoredPaths.size());
        UNIT_ASSERT_VALUES_EQUAL(10, requested.GetMutableMessage().GetValue());
        UNIT_ASSERT_VALUES_EQUAL(
            11,
            requested.GetMutableMessage().GetNested().GetMutable());
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            requested.GetMutableMessage().GetNested().GetFrozen());
        UNIT_ASSERT(requested.GetMutableMessage().HasSecond());
        UNIT_ASSERT_VALUES_EQUAL(
            30,
            requested.GetMutableMessage().GetExplicitFalse());
        UNIT_ASSERT_VALUES_EQUAL(
            40,
            requested.GetAtomicRecords(0).GetUnmarked());

        // Switch to a scalar, then use static as runtime to recover its choice.
        requested.SetMutableNumber(50);
        FilterRuntimeConfig(staticConfig, startup, requested);
        UNIT_ASSERT(requested.HasMutableNumber());
        requested = staticConfig;
        FilterRuntimeConfig(staticConfig, startup, requested);
        UNIT_ASSERT(requested.HasMutableMessage());
        UNIT_ASSERT_VALUES_EQUAL(10, requested.GetMutableMessage().GetValue());
        UNIT_ASSERT(!requested.GetMutableMessage().HasFirst());
        UNIT_ASSERT(!requested.GetMutableMessage().HasSecond());
    }

    // Verify that a switchable message keeps the requested child presence and
    // explicit scalar defaults while unknown data and startup values disappear.
    Y_UNIT_TEST(ShouldPreservePresenceInsideSwitchableOneof)
    {
        // Give startup a child whose fields have no runtime permission.
        const NProto::NTest::TRuntimeConfig staticConfig;
        NProto::NTest::TRuntimeConfig startup;
        startup.MutableMutableMessage()->MutableNested()->SetFrozen(1);
        startup.MutableMutableMessage()->SetFirst(2);

        for (const bool hasNested: {false, true}) {
            // Request either an absent or explicitly empty child and retain
            // the expected known state before adding unknown payloads.
            NProto::NTest::TRuntimeConfig requested;
            requested.MutableMutableMessage()->SetValue(0);
            if (hasNested) {
                requested.MutableMutableMessage()->MutableNested();
            }
            const auto expected = requested;
            AddUnknownField(*requested.MutableMutableMessage(), 3);
            if (hasNested) {
                AddUnknownField(
                    *requested.MutableMutableMessage()->MutableNested(),
                    4);
            }

            // Accept the known state as one value without restoring startup
            // children or changing presence during unknown-field cleanup.
            const auto diagnostics =
                FilterRuntimeConfig(staticConfig, startup, requested);
            UNIT_ASSERT(
                google::protobuf::util::MessageDifferencer::Equals(
                    expected,
                    requested));
            UNIT_ASSERT(diagnostics.IgnoredPaths.empty());
        }
    }

    // Verify that outer permissions block both oneof modes and that a nested
    // fixed oneof still applies its own field permissions outside compound
    // values.
    Y_UNIT_TEST(ShouldRespectOneofAncestorPermissions)
    {
        // Prepare the same kinds of choice below open and closed containers.
        NProto::NTest::TRuntimeConfig startup;
        startup.MutableAllowedChild()->MutableFixedMessage()->SetValue(10);
        startup.MutableAllowedChild()->MutableFixedMessage()->SetFrozen(20);
        startup.MutableFrozenChild()->SetMutableNumber(30);
        startup.MutableFrozenChild()->MutableFixedMessage()->SetValue(40);
        auto requested = startup;
        requested.MutableAllowedChild()->MutableFixedMessage()->SetValue(11);
        requested.MutableAllowedChild()->MutableFixedMessage()->SetFrozen(21);
        requested.MutableFrozenChild()->MutableMutableMessage()->SetValue(31);
        requested.MutableFrozenChild()->MutableFixedMessage()->SetValue(41);

        // Apply the nested permitted leaf only through the fully allowed path.
        const auto diagnostics =
            FilterRuntimeConfig(startup, startup, requested);
        UNIT_ASSERT_VALUES_EQUAL(
            11,
            requested.GetAllowedChild().GetFixedMessage().GetValue());
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            requested.GetAllowedChild().GetFixedMessage().GetFrozen());
        UNIT_ASSERT(requested.GetFrozenChild().HasMutableNumber());
        UNIT_ASSERT_VALUES_EQUAL(
            30,
            requested.GetFrozenChild().GetMutableNumber());
        UNIT_ASSERT_VALUES_EQUAL(
            40,
            requested.GetFrozenChild().GetFixedMessage().GetValue());

        // Attribute rejection to the forbidden ancestor for either oneof mode.
        UNIT_ASSERT_VALUES_EQUAL(3, diagnostics.IgnoredPaths.size());
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("AllowedChild.FixedMessage.Frozen") ==
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("FrozenChild.MutableChoice") ==
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("FrozenChild.FixedChoice") ==
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
    }

    // Verify that compound permissions discard unknown singular data silently,
    // including when a switch creates a message absent from startup.
    Y_UNIT_TEST(ShouldFilterUnknownFieldsInsideSwitchableMessages)
    {
        // Attach unknown startup data to the selected message and its child.
        NProto::NTest::TRuntimeConfig staticConfig;
        NProto::NTest::TRuntimeConfig startup;
        auto* value = startup.MutableMutableMessage();
        value->GetReflection()->MutableUnknownFields(value)->AddVarint(100, 1);
        auto* nested = value->MutableNested();
        nested->GetReflection()->MutableUnknownFields(nested)->AddVarint(
            101,
            2);
        NProto::NTest::TRuntimeConfig requested;
        requested.MutableMutableMessage()->SetValue(30);
        requested.MutableMutableMessage()->MutableNested()->SetFrozen(40);

        // Discard unknown data while allowing known fields without their
        // markers.
        const auto diagnostics =
            FilterRuntimeConfig(staticConfig, startup, requested);
        const auto& result = requested.GetMutableMessage();
        UNIT_ASSERT_VALUES_EQUAL(30, result.GetValue());
        UNIT_ASSERT_VALUES_EQUAL(40, result.GetNested().GetFrozen());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            result.GetReflection()->GetUnknownFields(result).field_count());
        const auto& resultNested = result.GetNested();
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            resultNested.GetReflection()
                ->GetUnknownFields(resultNested)
                .field_count());
        UNIT_ASSERT(diagnostics.IgnoredPaths.empty());

        // A new branch has no startup unknown data; discard incoming unknowns
        // without clearing the selected empty message.
        startup.SetMutableNumber(5);
        requested.Clear();
        value = requested.MutableMutableMessage();
        value->GetReflection()->MutableUnknownFields(value)->AddVarint(102, 3);
        FilterRuntimeConfig(staticConfig, startup, requested);
        UNIT_ASSERT(requested.HasMutableMessage());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            requested.GetMutableMessage().ByteSizeLong());
    }

    // Verify that synthetic proto3 optional oneofs retain ordinary field
    // behavior, including runtime activation and removal.
    Y_UNIT_TEST(ShouldTreatSyntheticOneofAsSingularField)
    {
        // Build an optional scalar represented by a synthetic oneof.
        google::protobuf::FileDescriptorProto file;
        file.set_name("runtime_synthetic_oneof.proto");
        file.set_syntax("proto3");
        file.add_dependency("cloud/storage/core/config/markers.proto");
        auto* message = file.add_message_type();
        message->set_name("TSyntheticOneof");
        message->add_oneof_decl()->set_name("_Value");
        auto* field = message->add_field();
        field->set_name("Value");
        field->set_number(1);
        field->set_type(google::protobuf::FieldDescriptorProto::TYPE_UINT32);
        field->set_label(
            google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
        field->set_oneof_index(0);
        field->set_proto3_optional(true);
        field->mutable_options()->SetExtension(
            NMarkers::AllowRuntimeUpdate,
            true);
        google::protobuf::DescriptorPool pool(
            google::protobuf::DescriptorPool::generated_pool());
        const auto* descriptor = pool.BuildFile(file)->message_type(0);
        google::protobuf::DynamicMessageFactory factory(&pool);
        const auto* prototype = factory.GetPrototype(descriptor);
        std::unique_ptr<google::protobuf::Message> startup(prototype->New());
        std::unique_ptr<google::protobuf::Message> requested(prototype->New());
        const auto* value = descriptor->field(0);

        // Build the ordinary field path without exposing its synthetic group.
        TRuntimeConfigPath path;
        path.Enter(*value);
        UNIT_ASSERT_VALUES_EQUAL("Value", path.GetPath());
        path.Leave();
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            path.Enter(*value->containing_oneof()),
            std::exception,
            "Cannot enter synthetic oneof 'TSyntheticOneof._Value'");
        UNIT_ASSERT_VALUES_EQUAL("", path.GetPath());

        // Activate an absent optional field, then remove it without fixing its
        // case.
        requested->GetReflection()->SetUInt32(requested.get(), value, 0);
        FilterRuntimeConfig(*startup, *startup, *requested);
        UNIT_ASSERT(requested->GetReflection()->HasField(*requested, value));
        startup->CopyFrom(*requested);
        requested->Clear();
        FilterRuntimeConfig(*startup, *startup, *requested);
        UNIT_ASSERT(!requested->GetReflection()->HasField(*requested, value));
    }

    // Verify that absent and explicit-false markers on switchable members are
    // schema errors even when the unmarked member is not selected.
    Y_UNIT_TEST(ShouldRejectUnmarkedSwitchableMembers)
    {
        // Build each invalid schema independently from configuration values.
        for (const bool explicitFalse: {false, true}) {
            google::protobuf::FileDescriptorProto file;
            file.set_name("runtime_invalid_oneof.proto");
            file.add_dependency("cloud/storage/core/config/markers.proto");
            auto* message = file.add_message_type();
            message->set_name("TInvalidOneof");
            auto* oneof = message->add_oneof_decl();
            oneof->set_name("Choice");
            oneof->mutable_options()->SetExtension(
                NMarkers::AllowRuntimeSwitch,
                true);
            for (int i = 0; i < 2; ++i) {
                auto* field = message->add_field();
                field->set_name("Member" + ToString(i));
                field->set_number(i + 1);
                field->set_type(
                    google::protobuf::FieldDescriptorProto::TYPE_UINT32);
                field->set_label(
                    google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
                field->set_oneof_index(0);
                if (i == 0 || explicitFalse) {
                    field->mutable_options()->SetExtension(
                        NMarkers::AllowRuntimeUpdate,
                        i == 0);
                }
            }
            google::protobuf::DescriptorPool pool(
                google::protobuf::DescriptorPool::generated_pool());
            const auto* descriptor = pool.BuildFile(file)->message_type(0);

            // Report the offending member and required marker to the caller.
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                ValidateRuntimeConfigSchema(*descriptor),
                std::exception,
                "Switchable oneof member 'TInvalidOneof.Member1' requires "
                "AllowRuntimeUpdate=true; the marker is false or absent");
        }
    }

    // Verify explicit false/empty values and prevent forbidden children from
    // creating messages that were absent at startup.
    Y_UNIT_TEST(ShouldPreserveScalarAndMessagePresence)
    {
        // Retain one explicitly present empty frozen container at startup.
        NProto::NTest::TRuntimeConfig startup;
        startup.MutableFrozen();
        NProto::NTest::TRuntimeConfig requested;
        requested.SetFlag(false);
        requested.SetText("");
        requested.MutableAllowed()->SetFrozen(0);
        requested.MutableExplicitFalse()->SetMutable(0);

        // Keep allowed scalar presence and restore the frozen message shape.
        const auto diagnostics =
            FilterRuntimeConfig(startup, startup, requested);
        UNIT_ASSERT(requested.HasFlag());
        UNIT_ASSERT(!requested.GetFlag());
        UNIT_ASSERT(requested.HasText());
        UNIT_ASSERT_VALUES_EQUAL("", requested.GetText());
        UNIT_ASSERT(requested.HasFrozen());
        UNIT_ASSERT(!requested.HasAllowed());
        UNIT_ASSERT(!requested.HasExplicitFalse());
        UNIT_ASSERT_VALUES_EQUAL(3, diagnostics.IgnoredPaths.size());
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("Allowed.Frozen") ==
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("Frozen") ==
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("ExplicitFalse.Mutable") ==
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
    }

    // Verify silent unknown removal beside an allowed known update.
    Y_UNIT_TEST(ShouldDiscardUnknownFieldsWithoutDiagnostics)
    {
        // Add startup unknown data at the root and below an allowed container.
        NProto::NTest::TRuntimeConfig startup;
        startup.GetReflection()->MutableUnknownFields(&startup)->AddVarint(
            100,
            1);
        auto* nested = startup.MutableAllowed();
        nested->GetReflection()->MutableUnknownFields(nested)->AddVarint(
            101,
            2);

        // Supply different unknown values beside an allowed known parameter.
        NProto::NTest::TRuntimeConfig requested;
        requested.GetReflection()
            ->MutableUnknownFields(&requested)
            ->AddVarint(102, 3);
        requested.MutableAllowed()->SetMutable(4);
        const auto diagnostics =
            FilterRuntimeConfig(startup, startup, requested);

        // Discard unknown data without discarding the known update.
        UNIT_ASSERT_VALUES_EQUAL(4, requested.GetAllowed().GetMutable());
        const auto& unknown =
            requested.GetReflection()->GetUnknownFields(requested);
        UNIT_ASSERT_VALUES_EQUAL(0, unknown.field_count());
        const auto& nestedUnknown =
            requested.GetAllowed().GetReflection()->GetUnknownFields(
                requested.GetAllowed());
        UNIT_ASSERT_VALUES_EQUAL(0, nestedUnknown.field_count());
        UNIT_ASSERT(diagnostics.IgnoredPaths.empty());
    }

    // Verify that unknown-only differences in atomic values do not produce
    // diagnostics, while explicit presence of known fields still matters.
    Y_UNIT_TEST(ShouldIgnoreUnknownOnlyChangesInCompoundFields)
    {
        // Retain equal known values in collections and a frozen oneof.
        NProto::NTest::TRuntimeConfig expected;
        expected.AddFrozenRecords()->SetFrozen(1);
        (*expected.MutableFrozenMessageMap())["ro"].SetFrozen(2);
        (*expected.MutableMessageMap())["rw"].SetMutable(3);
        expected.MutableFrozenChild()->MutableMutableMessage()->SetValue(4);
        auto startup = expected;
        AddUnknownField(*startup.MutableFrozenRecords(0), 10);
        AddUnknownField((*startup.MutableFrozenMessageMap())["ro"], 20);
        AddUnknownField(
            *startup.MutableFrozenChild()->MutableMutableMessage(),
            30);
        const auto startupBytes = startup.SerializeAsString();

        // Exercise both generated map access and the repeated representation.
        for (bool repeatedMap: {false, true}) {
            auto requested = expected;
            AddUnknownField(*requested.MutableFrozenRecords(0), 40);
            AddUnknownField(
                *requested.MutableFrozenChild()->MutableMutableMessage(),
                50);
            for (const auto* name: {"MessageMap", "FrozenMessageMap"}) {
                const auto* field =
                    requested.GetDescriptor()->FindFieldByName(name);
                if (repeatedMap) {
                    auto* entry =
                        requested.GetReflection()->MutableRepeatedMessage(
                            &requested,
                            field,
                            0);
                    const auto* value =
                        entry->GetDescriptor()->FindFieldByName("value");
                    AddUnknownField(*entry, 60);
                    AddUnknownField(
                        *entry->GetReflection()->MutableMessage(entry, value),
                        70);
                } else if (TStringBuf(name) == "MessageMap") {
                    AddUnknownField((*requested.MutableMessageMap())["rw"], 80);
                } else {
                    AddUnknownField(
                        (*requested.MutableFrozenMessageMap())["ro"],
                        90);
                }
            }

            // Remove all unknown data without reporting equal known values.
            const auto diagnostics =
                FilterRuntimeConfig(startup, startup, requested);
            UNIT_ASSERT(diagnostics.IgnoredPaths.empty());
            UNIT_ASSERT(
                google::protobuf::util::MessageDifferencer::Equals(
                    expected,
                    requested));
            UNIT_ASSERT_VALUES_EQUAL(startupBytes, startup.SerializeAsString());
        }

        // Keep strict presence comparison inside an atomic collection.
        auto requested = expected;
        requested.MutableFrozenRecords(0)->SetMutable(0);
        const auto diagnostics =
            FilterRuntimeConfig(startup, startup, requested);
        UNIT_ASSERT_VALUES_EQUAL(1, diagnostics.IgnoredPaths.size());
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("FrozenRecords[]") ==
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
        UNIT_ASSERT(!requested.GetFrozenRecords(0).HasMutable());
        UNIT_ASSERT(
            google::protobuf::util::MessageDifferencer::Equals(
                expected,
                requested));
    }

    // Verify that restoring either a static or startup oneof branch cannot
    // return unknown data to runtime or mutate static/startup.
    Y_UNIT_TEST(ShouldCleanRestoredOneofBranches)
    {
        for (bool readOnly: {false, true}) {
            // Give static and startup distinct known and unknown values.
            NProto::NTest::TRuntimeConfig staticConfig;
            auto* base = readOnly ? staticConfig.MutableReadOnlyMessage()
                                  : staticConfig.MutableFixedMessage();
            base->SetValue(10);
            AddUnknownField(*base, 100);
            auto startup = staticConfig;
            auto* initial = readOnly ? startup.MutableReadOnlyMessage()
                                     : startup.MutableFixedMessage();
            initial->SetValue(20);
            initial->SetFrozen(30);
            AddUnknownField(*initial, 200);
            const auto staticBytes = staticConfig.SerializeAsString();
            const auto startupBytes = startup.SerializeAsString();

            // Reject a switch and recover the original message without
            // unknown data.
            NProto::NTest::TRuntimeConfig requested;
            requested.SetFixedNumber(99);
            const auto diagnostics =
                FilterRuntimeConfig(staticConfig, startup, requested);
            const auto& restored = readOnly ? requested.GetReadOnlyMessage()
                                            : requested.GetFixedMessage();
            UNIT_ASSERT_VALUES_EQUAL(readOnly ? 20 : 10, restored.GetValue());
            UNIT_ASSERT_VALUES_EQUAL(30, restored.GetFrozen());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                restored.GetReflection()
                    ->GetUnknownFields(restored)
                    .field_count());
            UNIT_ASSERT(
                diagnostics.IgnoredPaths.at("FixedChoice") ==
                ERuntimeConfigIgnoreReason::OneofSwitchForbidden);
            UNIT_ASSERT_VALUES_EQUAL(
                staticBytes,
                staticConfig.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(startupBytes, startup.SerializeAsString());
        }
    }

    // Verify that unknown bytes cannot keep a container that only carried
    // rejected known fields, while explicit empty message presence survives.
    Y_UNIT_TEST(ShouldDiscardUnknownBeforeCheckingMessagePresence)
    {
        // Introduce a forbidden value and unknown bytes in a mutable container.
        NProto::NTest::TRuntimeConfig startup;
        NProto::NTest::TRuntimeConfig requested;
        requested.MutableAllowed()->SetFrozen(42);
        AddUnknownField(*requested.MutableAllowed(), 1);

        // Remove the container after rejecting its only known value.
        const auto diagnostics =
            FilterRuntimeConfig(startup, startup, requested);
        UNIT_ASSERT(!requested.HasAllowed());
        UNIT_ASSERT_VALUES_EQUAL(1, diagnostics.IgnoredPaths.size());
        UNIT_ASSERT(
            diagnostics.IgnoredPaths.at("Allowed.Frozen") ==
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);

        // Retain explicit presence when unknown data is the only payload.
        requested.MutableAllowed();
        AddUnknownField(*requested.MutableAllowed(), 2);
        const auto emptyDiagnostics =
            FilterRuntimeConfig(startup, startup, requested);
        UNIT_ASSERT(requested.HasAllowed());
        UNIT_ASSERT_VALUES_EQUAL(0, requested.GetAllowed().ByteSizeLong());
        UNIT_ASSERT(emptyDiagnostics.IgnoredPaths.empty());
    }

    // Verify that diagnostics retain every rejected path beyond 100 fields.
    Y_UNIT_TEST(ShouldReportAllDiagnosticPaths)
    {
        // Build a message with more parameters than the former diagnostic cap.
        google::protobuf::FileDescriptorProto file;
        file.set_name("runtime_diagnostic_limit.proto");
        auto* message = file.add_message_type();
        message->set_name("TManyParameters");
        for (int i = 1; i <= 120; ++i) {
            auto* field = message->add_field();
            field->set_name("Parameter" + ToString(i));
            field->set_number(i);
            field->set_type(
                google::protobuf::FieldDescriptorProto::TYPE_UINT32);
            field->set_label(
                google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
        }
        google::protobuf::DescriptorPool pool;
        const auto* descriptor = pool.BuildFile(file)->message_type(0);
        google::protobuf::DynamicMessageFactory factory(&pool);
        const auto* prototype = factory.GetPrototype(descriptor);
        std::unique_ptr<google::protobuf::Message> startup(prototype->New());
        std::unique_ptr<google::protobuf::Message> requested(prototype->New());
        for (int i = 0; i < descriptor->field_count(); ++i) {
            requested->GetReflection()->SetUInt32(
                requested.get(),
                descriptor->field(i),
                i);
        }

        // Restore every field and retain every path for monitoring lookup.
        const auto diagnostics =
            FilterRuntimeConfig(*startup, *startup, *requested);
        UNIT_ASSERT_VALUES_EQUAL(120, diagnostics.IgnoredPaths.size());
        for (int i = 1; i <= 120; ++i) {
            UNIT_ASSERT_C(
                diagnostics.IgnoredPaths.at("Parameter" + ToString(i)) ==
                    ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden,
                "Incorrect diagnostic reason for Parameter" << i);
        }
        UNIT_ASSERT_VALUES_EQUAL(0, requested->ByteSizeLong());
    }
}

}   // namespace NCloud::NConfig
