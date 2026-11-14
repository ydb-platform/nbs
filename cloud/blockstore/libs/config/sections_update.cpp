/*******************************************************************************

  * Section updates use explicit updaters shared by startup and runtime.

  * Every updater owns its section's validation and fallback. Diagnostics retain
    updater order and distinguish skipped entries from rejected sections.

  * No live configuration or logging state is changed here.

*******************************************************************************/

#include "sections_update.h"

#include "sections_update/features.h"
#include "sections_update/section_updater.h"
#include "sections_update/volume_throttling.h"

#include <util/generic/algorithm.h>
#include <util/generic/array_ref.h>
#include <util/generic/strbuf.h>
#include <util/string/builder.h>

#include <utility>

namespace NCloud::NBlockStore {

namespace {

// Section registration shared by preparation and merge passes.
// The section module owns the stateless updater for the lifetime of the process.
struct TConfigSectionUpdaterEntry
{
    // Diagnostic section path backed by a string literal.
    TStringBuf Path;

    // Section updater owned by its module and valid for the process lifetime.
    const IConfigSectionUpdater& Updater;
};

// Return the same ordered section updaters for preparation and merging.
TArrayRef<const TConfigSectionUpdaterEntry> GetConfigSectionUpdaters()
{
    static const TConfigSectionUpdaterEntry updaters[] = {
        {"Features", GetFeaturesSectionUpdater()},
        {"VolumeThrottling", GetVolumeThrottlingSectionUpdater()},
    };
    return updaters;
}

}   // namespace

bool TConfigSectionsUpdateIssues::HasRejectedSections() const
{
    return AnyOf(
        Sections,
        [](const auto& section) { return HasError(section.RejectionError); });
}

TConfigSectionsUpdateIssues PrepareDynamicConfigSectionsUpdate(
    NProto::TBlockstoreConfig& dynamicConfigCandidate,
    const NProto::TBlockstoreConfig* previousAcceptedDynamicConfig)
{
    TConfigSectionsUpdateIssues result;

    // Give each updater its own issue buffer and collect rejections without
    // preventing the remaining sections from being prepared.
    for (const auto& entry: GetConfigSectionUpdaters()) {
        TConfigSectionUpdateIssues section;
        section.Path = entry.Path;
        section.RejectionError = entry.Updater.Prepare(
            dynamicConfigCandidate,
            previousAcceptedDynamicConfig,
            section.Issues);
        if (!section.Issues.empty() || HasError(section.RejectionError)) {
            result.Sections.push_back(std::move(section));
        }
    }
    return result;
}

void MergeConfigSections(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    NProto::TBlockstoreConfig& result)
{
    for (const auto& entry: GetConfigSectionUpdaters()) {
        entry.Updater.Merge(staticConfig, dynamicConfig, result);
    }
}

TString FormatConfigSectionRejection(
    const TConfigSectionUpdateIssues& section)
{
    return TStringBuilder()
           << "Rejected invalid " << section.Path
           << " configuration: " << section.RejectionError.GetMessage();
}

}   // namespace NCloud::NBlockStore
