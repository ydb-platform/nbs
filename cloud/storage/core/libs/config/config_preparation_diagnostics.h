/*******************************************************************************

Configuration preparation diagnostics.

Reports own schema paths and ignore reasons without recording configuration
values or collection keys.

*******************************************************************************/

#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NCloud::NConfig {

// Reason for an ignored value or rejected change at a schema path.
enum class EConfigIgnoreReason
{
    // Runtime updates are forbidden by the field or an ancestor's marker.
    RuntimeUpdateForbidden,

    // The oneof selection is fixed at startup, including an unset selection.
    OneofSwitchForbidden,

    // Values must come from static configuration under the current policy and
    // static base. Restarting with the same base does not allow dynamic values.
    StaticOnly,
};

// Ignored values and rejected changes from configuration preparation.
// The report owns its paths and reasons. Paths contain schema names only;
// parameter values and collection keys are never recorded.
struct TConfigPreparationDiagnostics
{
    // Ignored values or rejected changes by schema path, one reason per path
    // without a count limit.
    //
    // Build lookup keys with TConfigSchemaPath during your own traversal:
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
    THashMap<TString, EConfigIgnoreReason> IgnoredPaths;
};

}   // namespace NCloud::NConfig
