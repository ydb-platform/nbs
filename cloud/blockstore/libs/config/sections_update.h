/*******************************************************************************

Blockstore configuration section updates: preparation and source merging.

  * Preparation modifies only a separate dynamic candidate and returns
    diagnostics grouped by section.

  * Rejected sections recover their previous accepted dynamic value.
    Startup uses no previous value.

  * Source composition runs after the standard protobuf merge.

  * Callers implement logging, publication and delivery acknowledgement.

*******************************************************************************/

#pragma once

#include <cloud/blockstore/config/blockstore.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

// Diagnostics for one section with skipped entries or a rejected update.
// Each record owns its issue strings and error and is returned to the caller.
struct TConfigSectionUpdateIssues
{
    // Aggregate section path, such as VolumeThrottling.
    TString Path;

    // Nonfatal issues found while preparing this section.
    TVector<TString> Issues;

    // Error explaining why the incoming section was rejected:
    //  -  HasError() for rejected sections.
    //  - !HasError() for fully or partially accepted sections.
    //  - Issues may be nonempty in either case.
    NCloud::NProto::TError RejectionError;
};

// Diagnostics owned by one preparation call.
// Sections without issues not included.
struct TConfigSectionsUpdateIssues
{
    // Sections with issues or a rejection.
    TVector<TConfigSectionUpdateIssues> Sections;

    // Check whether any incoming section was rejected.
    [[nodiscard]] bool HasRejectedSections() const;
};

// Prepare dynamicConfigCandidate, restoring rejected sections from
// previousAcceptedDynamicConfig - the last accepted dynamic configuration
// before merging with static configuration.
// Pass nullptr at startup; the two inputs must not alias.
TConfigSectionsUpdateIssues PrepareDynamicConfigSectionsUpdate(
    NProto::TBlockstoreConfig& dynamicConfigCandidate,
    const NProto::TBlockstoreConfig* previousAcceptedDynamicConfig);

// Apply section-specific merge rules to result.
// result must contain both configurations combined using protobuf MergeFrom().
// Modify only sections with custom merge rules; preserve all other sections.
void MergeConfigSections(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    NProto::TBlockstoreConfig& result);

// Format a rejected section's path and error; require HasError(RejectionError).
TString FormatConfigSectionRejection(const TConfigSectionUpdateIssues& section);

}   // namespace NCloud::NBlockStore
