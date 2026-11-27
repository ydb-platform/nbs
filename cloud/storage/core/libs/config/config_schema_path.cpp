#include "config_schema_path.h"

#include <util/generic/yexception.h>

#include <utility>

namespace NCloud::NConfig {

using google::protobuf::FieldDescriptor;
using google::protobuf::OneofDescriptor;

// Enter a field with its collection suffix and singular-message descent rule.
void TConfigSchemaPath::Enter(const FieldDescriptor& field)
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
void TConfigSchemaPath::Enter(const OneofDescriptor& oneof)
{
    Y_THROW_UNLESS(
        !oneof.is_synthetic(),
        "Cannot enter synthetic oneof '"
            << oneof.full_name()
            << "' in a config schema path; enter its field instead");
    Enter(oneof.name(), false);
}

// Append a complete frame only after validation, preserving the old path if
// allocating the segment or extending the stack throws.
void TConfigSchemaPath::Enter(TString segment, bool canDescend)
{
    Y_THROW_UNLESS(
        Frames.empty() || Frames.back().CanDescend,
        "Cannot enter '" << segment << "' below terminal config schema path '"
                         << GetPath()
                         << "'; only singular message fields allow descent");
    Frames.push_back({
        .Segment = std::move(segment),
        .CanDescend = canDescend,
    });
}

// Restore the parent without allocation; throw if already at the root.
void TConfigSchemaPath::Leave()
{
    Y_THROW_UNLESS(
        !Frames.empty(),
        "Cannot leave the root of a config schema path; "
        "each Leave must match a successful Enter");
    Frames.pop_back();
}

// Build an independent string without exposing storage inside the path stack.
TString TConfigSchemaPath::GetPath() const
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

}   // namespace NCloud::NConfig
