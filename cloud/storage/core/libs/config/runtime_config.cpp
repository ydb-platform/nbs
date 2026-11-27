/*******************************************************************************

Runtime filtering restores forbidden fields from the startup configuration.
Traversal carries the permission of all ancestors; collections and oneof
groups are restored as complete values. Diagnostics record the schema
path and reason for each rejected change to the runtime configuration.

*******************************************************************************/

#include "runtime_config.h"

#include "config_schema_path.h"

#include <cloud/storage/core/config/markers.pb.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

#include <google/protobuf/util/field_comparator.h>
#include <google/protobuf/util/message_differencer.h>

namespace NCloud::NConfig {

namespace {

////////////////////////////////////////////////////////////////////////////////

using google::protobuf::Descriptor;
using google::protobuf::FieldDescriptor;
using google::protobuf::Message;
using google::protobuf::OneofDescriptor;
using google::protobuf::util::MessageDifferencer;

// Permission carried into a message: Frozen restores startup values, Fields
// checks each marker. Unknown fields
// have already been removed from the runtime configuration before traversal.
enum class EUpdateMode
{
    Frozen,
    Fields,
};

bool AllowsUpdate(const FieldDescriptor* field)
{
    return field->options().GetExtension(NCloud::NMarkers::AllowRuntimeUpdate);
}

bool AllowsSwitch(const OneofDescriptor* oneof)
{
    return oneof->options().GetExtension(NCloud::NMarkers::AllowRuntimeSwitch);
}

// A stateless comparison policy owned by MessageDifferencer. Install it to
// ignore unknown data in static/startup while preserving presence comparison.
class TIgnoreUnknownFields final: public MessageDifferencer::IgnoreCriteria
{
public:
    bool IsIgnored(
        const Message&,
        const Message&,
        const FieldDescriptor*,
        const std::vector<MessageDifferencer::SpecificField>&) override
    {
        return false;
    }

    bool IsUnknownFieldIgnored(
        const Message&,
        const Message&,
        const MessageDifferencer::SpecificField&,
        const std::vector<MessageDifferencer::SpecificField>&) override
    {
        return true;
    }
};

// Remove unknown data from the message tree.
// Preserve known values and presence, keeping queued child messages valid.
void DiscardUnknownFields(Message& message)
{
    TVector<Message*> pending{&message};
    while (!pending.empty()) {
        auto* current = pending.back();
        pending.pop_back();
        const auto* reflection = current->GetReflection();
        reflection->MutableUnknownFields(current)->Clear();

        // Queue only present messages. Use repeated reflection for maps:
        // protobuf's built-in cleanup skips maps modified via this interface.
        std::vector<const FieldDescriptor*> fields;
        reflection->ListFields(*current, &fields);
        for (const auto* field: fields) {
            if (field->cpp_type() != FieldDescriptor::CPPTYPE_MESSAGE) {
                continue;
            }
            if (field->is_repeated()) {
                for (int i = 0; i < reflection->FieldSize(*current, field); ++i)
                {
                    pending.push_back(
                        reflection->MutableRepeatedMessage(current, field, i));
                }
            } else {
                pending.push_back(reflection->MutableMessage(current, field));
            }
        }
    }
}

// Record the reason for each rejected change for lookup by schema path.
void RecordChange(
    TConfigPreparationDiagnostics& diagnostics,
    TString path,
    EConfigIgnoreReason reason)
{
    diagnostics.IgnoredPaths.emplace(std::move(path), reason);
}

// Compare a complete parameter, including explicit presence and collection
// order; protobuf map comparison matches entries by key. Ignore unknown data
// recursively without treating absent fields as explicitly set defaults.
bool EqualField(
    const Message& lhs,
    const Message& rhs,
    const FieldDescriptor* field)
{
    if (!field->is_repeated() && lhs.GetReflection()->HasField(lhs, field) !=
                                     rhs.GetReflection()->HasField(rhs, field))
    {
        return false;
    }
    google::protobuf::util::DefaultFieldComparator comparator;
    comparator.set_treat_nan_as_equal(true);
    MessageDifferencer differencer;
    differencer.set_field_comparator(&comparator);
    differencer.AddIgnoreCriteria(std::make_unique<TIgnoreUnknownFields>());
    return differencer.CompareWithFields(lhs, rhs, {field}, {field});
}

// Require explicit permission on every switchable member, including inactive
// ones. Skip annotations inside compound values and stop recursive schemas.
void ValidateMessageSchema(
    const Descriptor* descriptor,
    THashSet<const Descriptor*>& visited)
{
    if (!visited.insert(descriptor).second) {
        return;
    }
    // Reject messages that allow extensions: the filter skips extension fields.
    Y_THROW_UNLESS(
        descriptor->extension_range_count() == 0,
        "Runtime config extensions are not supported: "
            << descriptor->full_name());

    for (int i = 0; i < descriptor->field_count(); ++i) {
        const auto* field = descriptor->field(i);
        if (const auto* oneof = field->real_containing_oneof();
            oneof && AllowsSwitch(oneof))
        {
            Y_THROW_UNLESS(
                AllowsUpdate(field),
                "Switchable oneof member '"
                    << field->full_name()
                    << "' requires AllowRuntimeUpdate=true; "
                       "the marker is false or absent");
            continue;
        }
        if (!field->is_repeated() &&
            field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE)
        {
            ValidateMessageSchema(field->message_type(), visited);
        }
    }
}

// Restore complete fields from startup, including oneof selection.
// Remove unknown data from the temporary copy before swapping, so restoration
// cannot return unknown fields to the runtime configuration.
void RestoreFields(
    const Message& source,
    Message& destination,
    const std::vector<const FieldDescriptor*>& fields)
{
    std::unique_ptr<Message> copy(destination.New());
    copy->CopyFrom(source);
    DiscardUnknownFields(*copy);
    destination.GetReflection()->SwapFields(&destination, copy.get(), fields);
}

// Set a field to its protobuf default and make it present.
// Clear message contents without creating child fields.
void SetFieldDefault(Message& message, const FieldDescriptor* field)
{
    const auto* reflection = message.GetReflection();
    switch (field->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            reflection->SetInt32(&message, field, field->default_value_int32());
            break;
        case FieldDescriptor::CPPTYPE_INT64:
            reflection->SetInt64(&message, field, field->default_value_int64());
            break;
        case FieldDescriptor::CPPTYPE_UINT32:
            reflection->SetUInt32(
                &message,
                field,
                field->default_value_uint32());
            break;
        case FieldDescriptor::CPPTYPE_UINT64:
            reflection->SetUInt64(
                &message,
                field,
                field->default_value_uint64());
            break;
        case FieldDescriptor::CPPTYPE_DOUBLE:
            reflection->SetDouble(
                &message,
                field,
                field->default_value_double());
            break;
        case FieldDescriptor::CPPTYPE_FLOAT:
            reflection->SetFloat(&message, field, field->default_value_float());
            break;
        case FieldDescriptor::CPPTYPE_BOOL:
            reflection->SetBool(&message, field, field->default_value_bool());
            break;
        case FieldDescriptor::CPPTYPE_ENUM:
            reflection->SetEnum(&message, field, field->default_value_enum());
            break;
        case FieldDescriptor::CPPTYPE_STRING:
            reflection->SetString(
                &message,
                field,
                field->default_value_string());
            break;
        case FieldDescriptor::CPPTYPE_MESSAGE:
            reflection->MutableMessage(&message, field)->Clear();
            break;
    }
}

// Fill missing required fields with defaults if runtime updates are allowed and
// return whether the result is initialized.
// Leave collections and switchable oneof values unchanged.
bool TryCompleteRequiredFields(Message& message)
{
    const auto* descriptor = message.GetDescriptor();
    const auto* reflection = message.GetReflection();

    for (int i = 0; i < descriptor->field_count(); ++i) {
        const auto* field = descriptor->field(i);

        // Preserve values that the filter accepts or restores as a whole.
        if (field->is_repeated() ||
            (field->real_containing_oneof() &&
             AllowsSwitch(field->real_containing_oneof())))
        {
            continue;
        }

        // Fill missing required field if allowed.
        // Otherwise, return false to preserve the field's StartupConfig state.
        if (field->is_required() && !reflection->HasField(message, field)) {
            if (!AllowsUpdate(field)) {
                return false;
            }
            SetFieldDefault(message, field);
        }

        // Fill required defaults only in present messages that allow runtime
        // updates.
        // Preserve StartupConfig state in messages whose runtime updates are
        // forbidden.
        if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE &&
            AllowsUpdate(field) && reflection->HasField(message, field))
        {
            if (!TryCompleteRequiredFields(
                    *reflection->MutableMessage(&message, field)))
            {
                return false;
            }
        }
    }

    return message.IsInitialized();
}

// References used to filter one message at a time. Create a context on the
// stack for each recursion level; all referenced objects must outlive the call.
// The context owns no data and shares the traversal's path and diagnostics.
struct TMessageFilterContext
{
    // Startup values for the same message, unchanged across runtime updates.
    const Message& StartupConfig;

    // Runtime message being filtered, separate from static and startup.
    Message& RuntimeConfig;

    // Current schema path; each entering scope must restore its parent path.
    TConfigSchemaPath& Path;

    // Rejected changes accumulated across all levels of the traversal.
    TConfigPreparationDiagnostics& Diagnostics;
};

// Filter one message and restore its queued fields after traversal.
void FilterMessage(TMessageFilterContext& context, EUpdateMode mode);

// Filter a singular message recursively, preserving its presence rules.
// Require the field's path to be entered; leave that path unchanged on return.
void FilterSingularMessageField(
    TMessageFilterContext& context,
    const FieldDescriptor& field,
    EUpdateMode mode)
{
    const auto& startup = context.StartupConfig;
    auto& runtime = context.RuntimeConfig;
    const auto* reflection = runtime.GetReflection();

    // Capture presence and emptiness before recursion can change the message.
    const bool initialPresent =
        startup.GetReflection()->HasField(startup, &field);
    const bool requestedPresent = reflection->HasField(runtime, &field);
    if (!initialPresent && !requestedPresent) {
        return;
    }
    const auto previousCount = context.Diagnostics.IgnoredPaths.size();
    auto* nested = reflection->MutableMessage(&runtime, &field);
    const bool requestedEmpty =
        mode == EUpdateMode::Fields && !HasKnownFields(*nested);

    // Recurse with the matching messages and the same path and diagnostics.
    TMessageFilterContext childContext{
        .StartupConfig = startup.GetReflection()->GetMessage(startup, &field),
        .RuntimeConfig = *nested,
        .Path = context.Path,
        .Diagnostics = context.Diagnostics,
    };
    FilterMessage(childContext, mode);

    // Preserve the existing presence rules before adding required defaults.
    const bool preserveRequestedPresence =
        requestedPresent &&
        (field.is_required() || requestedEmpty || initialPresent);

    const bool fieldCanBeCleared =
        mode == EUpdateMode::Fields && !field.real_containing_oneof() &&
        !preserveRequestedPresence && !HasKnownFields(*nested);

    // Fill missing required fields with defaults if runtime markers allow it
    // and the filter must keep the new message:
    // - it is required
    // - belongs to a oneof
    // - was explicitly requested empty
    // - or still has fields after filtering
    // Otherwise (its required fields cannot be initialized), record the
    // rejection and remove the message.
    const bool newContainerNeedsInitialization = mode == EUpdateMode::Fields &&
                                                 !initialPresent &&
                                                 !nested->IsInitialized();

    if (newContainerNeedsInitialization) {
        const bool initialized =
            !fieldCanBeCleared && TryCompleteRequiredFields(*nested);

        if (!initialized) {
            RecordChange(
                context.Diagnostics,
                context.Path.GetPath(),
                EConfigIgnoreReason::RuntimeUpdateForbidden);
            reflection->ClearField(&runtime, &field);
            return;
        }
    }

    // Keep the message present or absent as in StartupConfig when runtime
    // updates are forbidden. For allowed updates, remove empty messages omitted
    // from the request or introduced only by rejected fields. Keep explicitly
    // requested required messages and selected oneof members even when empty.
    if (mode == EUpdateMode::Frozen) {
        if (initialPresent != requestedPresent &&
            context.Diagnostics.IgnoredPaths.size() == previousCount)
        {
            RecordChange(
                context.Diagnostics,
                context.Path.GetPath(),
                EConfigIgnoreReason::RuntimeUpdateForbidden);
        }
        if (!initialPresent) {
            reflection->ClearField(&runtime, &field);
        }
    } else if (fieldCanBeCleared) {
        reflection->ClearField(&runtime, &field);
    }
}

// Apply a field's effective permission and queue fields to restore from
// StartupConfig after traversing the current message.
// Enter the field path for filtering and restore the parent path on return.
void FilterField(
    TMessageFilterContext& context,
    const FieldDescriptor& field,
    EUpdateMode mode,
    std::vector<const FieldDescriptor*>& frozenFields)
{
    // Check the field marker only when individual field permissions apply.
    if (mode == EUpdateMode::Fields && !AllowsUpdate(&field)) {
        mode = EUpdateMode::Frozen;
    }
    context.Path.Enter(field);
    Y_DEFER
    {
        context.Path.Leave();
    };

    // Recurse only through singular messages. Treat collections as complete
    // values so element markers cannot alter the collection's permission.
    if (!field.is_repeated() &&
        field.cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE)
    {
        FilterSingularMessageField(context, field, mode);
    } else if (
        mode == EUpdateMode::Frozen &&
        !EqualField(context.StartupConfig, context.RuntimeConfig, &field))
    {
        RecordChange(
            context.Diagnostics,
            context.Path.GetPath(),
            EConfigIgnoreReason::RuntimeUpdateForbidden);
        frozenFields.push_back(&field);
    }
}

// Filter oneof selection and contents according to the ancestor's mode.
// Queue rejected groups for complete restoration from startup after traversal.
// Filter member fields only when a fixed oneof keeps its startup selection.
void FilterOneof(
    TMessageFilterContext& context,
    const OneofDescriptor& oneof,
    EUpdateMode mode,
    std::vector<const FieldDescriptor*>& frozenFields)
{
    const auto& startup = context.StartupConfig;
    auto& runtime = context.RuntimeConfig;
    const auto* initial =
        startup.GetReflection()->GetOneofFieldDescriptor(startup, &oneof);
    const auto* current =
        runtime.GetReflection()->GetOneofFieldDescriptor(runtime, &oneof);

    // Restore a forbidden ancestor's complete choice without consulting
    // descendant permissions or reporting its individual fields.
    if (mode == EUpdateMode::Frozen) {
        if (initial != current ||
            (initial && !EqualField(startup, runtime, initial)))
        {
            context.Path.Enter(oneof);
            Y_DEFER
            {
                context.Path.Leave();
            };
            RecordChange(
                context.Diagnostics,
                context.Path.GetPath(),
                EConfigIgnoreReason::RuntimeUpdateForbidden);
            frozenFields.push_back(oneof.field(0));
        }
        return;
    }

    // Accept switchable contents as one value. Unknown fields were removed
    // before traversal, and descendant markers do not constrain this value.
    if (AllowsSwitch(&oneof)) {
        return;
    }

    // Reject the whole group when its selected member changes.
    if (initial != current) {
        context.Path.Enter(oneof);
        Y_DEFER
        {
            context.Path.Leave();
        };
        RecordChange(
            context.Diagnostics,
            context.Path.GetPath(),
            EConfigIgnoreReason::OneofSwitchForbidden);
        frozenFields.push_back(oneof.field(0));
        return;
    }

    // Apply field permissions within the unchanged startup member.
    if (initial) {
        FilterField(context, *initial, EUpdateMode::Fields, frozenFields);
    }
}

// Filter one message in schema order, visiting each real oneof once.
// Restore queued values only after all comparisons at this message level.
void FilterMessage(TMessageFilterContext& context, EUpdateMode mode)
{
    const auto* descriptor = context.RuntimeConfig.GetDescriptor();
    std::vector<const FieldDescriptor*> frozenFields;

    // Visit ordinary fields and whole oneof groups in the same schema order.
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const auto* field = descriptor->field(i);
        if (const auto* oneof = field->real_containing_oneof()) {
            if (field == oneof->field(0)) {
                FilterOneof(context, *oneof, mode, frozenFields);
            }
            continue;
        }
        FilterField(context, *field, mode, frozenFields);
    }

    // Restore complete frozen fields from the startup configuration.
    // Restore each real oneof as a whole, including an unset selection.
    if (!frozenFields.empty()) {
        RestoreFields(
            context.StartupConfig,
            context.RuntimeConfig,
            frozenFields);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

// Validate a configuration schema in tests, including inactive message types.
// Skip inner annotations of compound values and visit each message type once.
void ValidateRuntimeConfigSchema(const Descriptor& descriptor)
{
    THashSet<const Descriptor*> visited;
    ValidateMessageSchema(&descriptor, visited);
}

// Restore forbidden runtime values from startup and report rejected changes.
TConfigPreparationDiagnostics FilterRuntimeConfig(
    const Message& staticConfig,
    const Message& startupConfig,
    Message& runtimeConfig)
{
    // Reject incompatible descriptors before modifying the runtime input.
    Y_THROW_UNLESS(
        startupConfig.GetDescriptor() == staticConfig.GetDescriptor(),
        "Startup config type mismatch: expected static protobuf type '"
            << staticConfig.GetDescriptor()->full_name()
            << "', got startup type '"
            << startupConfig.GetDescriptor()->full_name() << "'");
    Y_THROW_UNLESS(
        runtimeConfig.GetDescriptor() == staticConfig.GetDescriptor(),
        "Runtime config type mismatch: expected static protobuf type '"
            << staticConfig.GetDescriptor()->full_name()
            << "', got runtime type '"
            << runtimeConfig.GetDescriptor()->full_name() << "'");

    // Reject missing required startup fields before modifying runtime.
    Y_THROW_UNLESS(
        startupConfig.IsInitialized(),
        "Startup config is missing required fields: "
            << startupConfig.InitializationErrorString());

    // Remove unknown fields before presence checks. RestoreFields also removes
    // them when restoring values from startup.
    DiscardUnknownFields(runtimeConfig);

    TConfigPreparationDiagnostics diagnostics;
    TConfigSchemaPath path;
    TMessageFilterContext context{
        .StartupConfig = startupConfig,
        .RuntimeConfig = runtimeConfig,
        .Path = path,
        .Diagnostics = diagnostics,
    };
    FilterMessage(context, EUpdateMode::Fields);

    // Complete retained messages after optional containers have been removed.
    if (!runtimeConfig.IsInitialized()) {
        const bool initialized = TryCompleteRequiredFields(runtimeConfig);

        Y_THROW_UNLESS(
            initialized,
            "Cannot restore required runtime config fields: "
                << runtimeConfig.InitializationErrorString());
    }
    return diagnostics;
}

}   // namespace NCloud::NConfig
