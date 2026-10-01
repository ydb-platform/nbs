/*******************************************************************************

Marker-based filtering of protobuf runtime configurations. The filter keeps
permitted changes, restores forbidden values from startup, and uses static
values when recovering a fixed oneof member. The caller supplies a complete
runtime configuration after merging inputs according to its own rules.
Schema paths identify rejected changes for lookup during a caller's traversal.
Validation errors throw yexception, derived from std::exception.

*******************************************************************************/

#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <google/protobuf/message.h>

namespace NCloud::NConfig {

// A schema path for a caller-controlled protobuf traversal. The helper owns
// a stack of name segments; it retains neither messages nor descriptors.
// Create one helper at the root, then pair each successful Enter() with Leave()
// in the same scope, preferably through Y_DEFER { path.Leave(); }.
// Pass descriptors from the current message; their containing type is not
// checked by the helper.
// Only singular message fields allow further descent. Collections and real
// oneof groups are terminal nodes; a oneof member uses its own field name.
class TRuntimeConfigPath
{
public:
    TRuntimeConfigPath() = default;
    TRuntimeConfigPath(const TRuntimeConfigPath&) = delete;
    TRuntimeConfigPath& operator=(const TRuntimeConfigPath&) = delete;
    TRuntimeConfigPath(TRuntimeConfigPath&&) = delete;
    TRuntimeConfigPath& operator=(TRuntimeConfigPath&&) = delete;

    // Enter a field with [] for collections; throw if the current node is
    // terminal.
    void Enter(const google::protobuf::FieldDescriptor& field);

    // Enter a real oneof; throw for synthetic groups or below a terminal node.
    void Enter(const google::protobuf::OneofDescriptor& oneof);

    // Return to the parent; throw if already at the root.
    void Leave();

    // Build an owning, dot-separated path; return an empty string at the root.
    TString GetPath() const;

private:
    // One entered node, owned by the path stack until the matching Leave().
    struct TFrame
    {
        // Exact proto name, with [] appended for a repeated or map field.
        TString Segment;

        // Permission to enter children: true only for singular messages.
        bool CanDescend;
    };

    // Append a node after checking that the current node permits descent.
    void Enter(TString segment, bool canDescend);

private:
    // Entered nodes in root-to-leaf order; an empty stack denotes the root.
    TVector<TFrame> Frames;
};

// Reason for a rejected change at a schema path returned by the runtime filter.
enum class ERuntimeConfigIgnoreReason
{
    // Runtime updates are forbidden by the field or an ancestor's marker.
    RuntimeUpdateForbidden,

    // The oneof selection is fixed at startup, including an unset selection.
    OneofSwitchForbidden,
};

// A complete report of rejected changes returned by FilterRuntimeConfig().
// The report owns its paths and reasons. Paths contain schema names only;
// parameter values and collection keys are never recorded.
struct TRuntimeConfigDiagnostics
{
    // Rejected changes by schema path, one reason per path without a count
    // limit. The caller can convert reasons to text for display.
    //
    // Build lookup keys with TRuntimeConfigPath during your own traversal:
    // 1. Call path.Enter(*field), then IgnoredPaths.find(path.GetPath()).
    // 2. Call path.Leave() after the field and its children; use Y_DEFER.
    // Include absent fields so rejected removals can also be found.
    //
    // Paths are relative to the root and use exact proto names:
    // - Parent.Field: a scalar value or a change in message presence.
    // - Parent.Items[]: the complete repeated/map field, without element paths.
    // - Parent.Choice: a oneof group; member paths use Parent.Member.Field.
    //
    // For a non-null field->real_containing_oneof(), enter the group from the
    // member's parent, look up its path, then leave before entering the member.
    // A rejected oneof switch (OneofSwitchForbidden) does not forbid allowed
    // updates within the selected member.
    //
    // Use a message's path + "." as a prefix to find its descendant paths.
    THashMap<TString, ERuntimeConfigIgnoreReason> IgnoredPaths;
};

// Reject inconsistent oneof markers outside compound parameter values.
// Call this function in a test for every configuration type used with
// FilterRuntimeConfig(), passing the root descriptor. Throw for an invalid
// schema; FilterRuntimeConfig() does not perform this check.
void ValidateRuntimeConfigSchema(
    const google::protobuf::Descriptor& descriptor);

// Filter the runtime configuration in place and report rejected changes.
//
// Inputs:
// - staticConfig: the base configuration used to build runtimeConfig.
// - startupConfig: the complete configuration accepted at startup. Keep it
//   unchanged across runtime updates.
// - runtimeConfig: the complete requested runtime state, with merging and
//   absent overrides resolved by the caller before this call.
//
// Outputs:
// - runtimeConfig retains permitted changes and restores forbidden state from
//   startupConfig. Fixed oneofs keep the startup selection; permitted fields
//   in a recovered member use its matching static value or protobuf defaults.
//   Unknown fields are removed recursively, without diagnostics. Unknown data
//   in staticConfig and startupConfig is ignored; both sources stay unchanged.
// - The return value maps rejected paths to reasons in IgnoredPaths.
//
// Pass runtimeConfig as a separate message from staticConfig and startupConfig
// (no aliasing); runtimeConfig is modified by this call.
// Require identical message descriptors and a marker schema checked in tests
// with ValidateRuntimeConfigSchema(). Throw on descriptor mismatch before
// modifying runtimeConfig. Other exceptions may leave it partly modified:
// discard it on failure. Both source configurations stay unchanged.
TRuntimeConfigDiagnostics FilterRuntimeConfig(
    const google::protobuf::Message& staticConfig,
    const google::protobuf::Message& startupConfig,
    google::protobuf::Message& runtimeConfig);

}   // namespace NCloud::NConfig
