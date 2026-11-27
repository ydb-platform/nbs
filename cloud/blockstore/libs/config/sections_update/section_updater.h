/*******************************************************************************

Configuration section preparation and merge contract. Each implementation reads
and changes only its own section and keeps configuration in call arguments.
Updaters must not retain arguments or use shared mutable state. Callers own
diagnostics, logging and publication.

*******************************************************************************/

#pragma once

#include <cloud/blockstore/config/blockstore.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

// Stateless thread-safe updater for one configuration section.
// Implementations own no configuration or call state.
//
// Required call order:
//  1. Prepare() must be called for all updaters to update
//     dynamicConfigCandidate.
//  2. The prepared candidate must be used as dynamicConfig in Merge().
//     The caller must initialize result from staticConfig and call
//     result.MergeFrom(dynamicConfig) before calling Merge().
//  3. Merge() must be called for all updaters. The updated result is the
//     effective configuration.
class IConfigSectionUpdater
{
public:
    virtual ~IConfigSectionUpdater() = default;

    // Validate the owned section of the incoming dynamic configuration and
    // prepare it for merging with static configuration.
    //
    // Possible outcomes:
    //  - Accepted: the section is valid and remains unchanged.
    //  - Accepted with issues: nonfatal problems are handled according to the
    //    section's policy, for example by skipping unsupported content.
    //    The resulting section is accepted.
    //  - Rejected: the entire incoming section is rejected.
    //
    // An absent incoming section is accepted and remains absent.
    // All other sections are preserved.
    //
    // @param   dynamicConfigCandidate - Incoming dynamic configuration.
    //          Contains the prepared section on success.
    //
    // @param   issues - Nonfatal issues detected during this call.
    //          The vector is cleared at the beginning of each call.
    //
    // @return  Success with empty issues if accepted without issues.
    //          Success with nonempty issues if accepted with issues.
    //          Error for a validation failure that rejects the incoming
    //          section:
    //              - the owned section in dynamicConfigCandidate may have been
    //                partially modified. It is the caller's responsibility to
    //                take corresponding actions (e.g. restore the section from
    //                the last accepted config)
    //              - issues may be nonempty
    //
    // Discard the entire dynamic configuration candidate if an exception is
    // thrown.
    [[nodiscard]] virtual NCloud::NProto::TError Prepare(
        NProto::TBlockstoreConfig& dynamicConfigCandidate,
        TVector<TString>& issues) const = 0;

    // Apply the owned section's merge policy to the combined configuration.
    //
    // Only the owned section in result is modified.
    // All other sections and both sources are preserved.
    // The merge policy does not depend on other updaters' output.
    //
    // @param   staticConfig - Static configuration source.
    //
    // @param   dynamicConfig - Dynamic configuration source before merging
    //          with static configuration.
    //
    // @param   result - Contains staticConfig and dynamicConfig combined using
    //          protobuf MergeFrom() on entry.
    //          On return, contains the combined configuration with the owned
    //          section merged according to its merge policy.
    //          Must not alias either source.
    virtual void Merge(
        const NProto::TBlockstoreConfig& staticConfig,
        const NProto::TBlockstoreConfig& dynamicConfig,
        NProto::TBlockstoreConfig& result) const = 0;
};

}   // namespace NCloud::NBlockStore
