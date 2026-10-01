/*******************************************************************************

Runtime filtering restores forbidden fields from the startup configuration.
Traversal carries the permission of all ancestors; collections and oneof
selection are restored as complete values. Diagnostics record the schema
path and reason for each rejected change to the runtime configuration.

*******************************************************************************/

#include "runtime_config.h"

#include <cloud/storage/core/config/markers.pb.h>

#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/generic/yexception.h>

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

// Remove unknown data from the message tree using an explicit stack.
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
    TRuntimeConfigDiagnostics& diagnostics,
    TString path,
    ERuntimeConfigIgnoreReason reason)
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
    MessageDifferencer differencer;
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

// Restore complete fields from static or startup, including oneof selection.
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

// Select the startup member with its protobuf default when the static
// configuration has no matching branch. Preserve zero or empty selections.
void SelectDefaultMember(Message& message, const FieldDescriptor* field)
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

// References used to filter one message at a time. Create a context on the
// stack for each recursion level; all referenced objects must outlive the call.
// The context owns no data and shares the traversal's path and diagnostics.
struct TMessageFilterContext
{
    // Static values for the message at the current traversal level.
    const Message& StaticConfig;

    // Startup values for the same message, unchanged across runtime updates.
    const Message& StartupConfig;

    // Runtime message being filtered, separate from static and startup.
    Message& RuntimeConfig;

    // Current schema path; each entering scope must restore its parent path.
    TRuntimeConfigPath& Path;

    // Rejected changes accumulated across all levels of the traversal.
    TRuntimeConfigDiagnostics& Diagnostics;
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
    const bool requestedEmpty = nested->ByteSizeLong() == 0;

    // Recurse with the matching messages and the same path and diagnostics.
    TMessageFilterContext childContext{
        .StaticConfig = context.StaticConfig.GetReflection()->GetMessage(
            context.StaticConfig,
            &field),
        .StartupConfig = startup.GetReflection()->GetMessage(startup, &field),
        .RuntimeConfig = *nested,
        .Path = context.Path,
        .Diagnostics = context.Diagnostics,
    };
    FilterMessage(childContext, mode);

    // Preserve forbidden presence; remove containers that existed only to
    // carry rejected fields, but retain a selected oneof message even if empty.
    if (mode == EUpdateMode::Frozen) {
        if (initialPresent != requestedPresent &&
            context.Diagnostics.IgnoredPaths.size() == previousCount)
        {
            RecordChange(
                context.Diagnostics,
                context.Path.GetPath(),
                ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
        }
        if (!initialPresent) {
            reflection->ClearField(&runtime, &field);
        }
    } else if (
        !field.real_containing_oneof() && nested->ByteSizeLong() == 0 &&
        (!requestedPresent || (!requestedEmpty && !initialPresent)))
    {
        reflection->ClearField(&runtime, &field);
    }
}

// Apply a field's effective permission and queue forbidden complete values.
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
            ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
        frozenFields.push_back(&field);
    }
}

// Restore the startup selection after a forbidden switch. A null initial
// member means unset; permitted members use static values or defaults, and
// forbidden members use startup. Leave field filtering to the caller.
void RestoreFixedOneofMember(
    TMessageFilterContext& context,
    const OneofDescriptor& oneof,
    const FieldDescriptor* initial)
{
    auto& runtime = context.RuntimeConfig;

    // Preserve an unset startup selection instead of choosing a runtime member.
    if (!initial) {
        runtime.GetReflection()->ClearOneof(&runtime, &oneof);
        return;
    }

    // Restore a forbidden member completely, ignoring descendant permissions.
    if (!AllowsUpdate(initial)) {
        RestoreFields(context.StartupConfig, runtime, {initial});
        return;
    }

    // Recover permitted values from the matching static member or defaults.
    const auto& staticConfig = context.StaticConfig;
    if (staticConfig.GetReflection()->GetOneofFieldDescriptor(
            staticConfig,
            &oneof) == initial)
    {
        RestoreFields(staticConfig, runtime, {initial});
    } else {
        SelectDefaultMember(runtime, initial);
    }
}

// Filter oneof selection and contents according to the ancestor's mode.
// Restore a fixed selection before filtering its member; keep group and member
// paths separate and queue completely forbidden choices for later restoration.
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
                ERuntimeConfigIgnoreReason::RuntimeUpdateForbidden);
            frozenFields.push_back(oneof.field(0));
        }
        return;
    }

    // Accept switchable contents as one value. Unknown fields were removed
    // before traversal, and descendant markers do not constrain this value.
    if (AllowsSwitch(&oneof)) {
        return;
    }

    // Recover the fixed branch before visiting its fields. Leave the group
    // path before entering the member, so both paths use the same parent.
    if (initial != current) {
        context.Path.Enter(oneof);
        Y_DEFER
        {
            context.Path.Leave();
        };
        RecordChange(
            context.Diagnostics,
            context.Path.GetPath(),
            ERuntimeConfigIgnoreReason::OneofSwitchForbidden);
        RestoreFixedOneofMember(context, oneof, initial);
    }
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

// Enter a field with its collection suffix and singular-message descent rule.
void TRuntimeConfigPath::Enter(const FieldDescriptor& field)
{
    TString segment = field.name();
    if (field.is_repeated()) {
        segment += "[]";
    }
    Enter(
        std::move(segment),
        !field.is_repeated() &&
            field.cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE);
}

// Enter a oneof selection path without making it a parent of member paths.
void TRuntimeConfigPath::Enter(const OneofDescriptor& oneof)
{
    Y_THROW_UNLESS(
        !oneof.is_synthetic(),
        "Cannot enter synthetic oneof '"
            << oneof.full_name()
            << "' in a runtime config path; enter its field instead");
    Enter(oneof.name(), false);
}

// Append a complete frame only after validation, preserving the old path if
// allocating the segment or extending the stack throws.
void TRuntimeConfigPath::Enter(TString segment, bool canDescend)
{
    Y_THROW_UNLESS(
        Frames.empty() || Frames.back().CanDescend,
        "Cannot enter '" << segment << "' below terminal runtime config path '"
                         << GetPath()
                         << "'; only singular message fields allow descent");
    Frames.push_back({
        .Segment = std::move(segment),
        .CanDescend = canDescend,
    });
}

// Restore the parent without allocation; throw if already at the root.
void TRuntimeConfigPath::Leave()
{
    Y_THROW_UNLESS(
        !Frames.empty(),
        "Cannot leave the root of a runtime config path; "
        "each Leave must match a successful Enter");
    Frames.pop_back();
}

// Build an independent string without exposing storage inside the path stack.
TString TRuntimeConfigPath::GetPath() const
{
    TString result;
    for (const auto& frame: Frames) {
        if (!result.empty()) {
            result += '.';
        }
        result += frame.Segment;
    }
    return result;
}

// Validate a configuration schema in tests, including inactive message types.
// Skip inner annotations of compound values and visit each message type once.
void ValidateRuntimeConfigSchema(const Descriptor& descriptor)
{
    THashSet<const Descriptor*> visited;
    ValidateMessageSchema(&descriptor, visited);
}

// Restore forbidden runtime values from startup and report rejected changes.
// Use static or defaults for permitted fields in recovered oneof members.
TRuntimeConfigDiagnostics FilterRuntimeConfig(
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

    // Remove unknown fields before presence checks. RestoreFields also removes
    // them when restoring values from static or startup.
    DiscardUnknownFields(runtimeConfig);

    TRuntimeConfigDiagnostics diagnostics;
    TRuntimeConfigPath path;
    TMessageFilterContext context{
        .StaticConfig = staticConfig,
        .StartupConfig = startupConfig,
        .RuntimeConfig = runtimeConfig,
        .Path = path,
        .Diagnostics = diagnostics,
    };
    FilterMessage(context, EUpdateMode::Fields);
    return diagnostics;
}

}   // namespace NCloud::NConfig
