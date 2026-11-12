/*******************************************************************************

  * Section updates use explicit updaters shared by startup and runtime.

  * Each updater validates and prepares its own section.

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
#include <util/system/yassert.h>

#include <google/protobuf/descriptor.h>

#include <utility>

namespace NCloud::NBlockStore {

namespace {

// Section registration shared by preparation and merge passes.
// The section module owns the stateless updater for the lifetime of the process.
struct TConfigSectionUpdaterEntry
{
    // Diagnostic section path backed by a string literal.
    TStringBuf Path;

    // Number of the optional TBlockstoreConfig proto message field this updater
    // owns.
    int FieldNumber;

    // Section updater owned by its module and valid for the process lifetime.
    const IConfigSectionUpdater& Updater;
};

// Return the same ordered section updaters for preparation and merging.
TArrayRef<const TConfigSectionUpdaterEntry> GetConfigSectionUpdaters()
{
    static const TConfigSectionUpdaterEntry updaters[] = {
        {
            "Features",
            NProto::TBlockstoreConfig::kFeaturesFieldNumber,
            GetFeaturesSectionUpdater(),
        },
        {
            "VolumeThrottling",
            NProto::TBlockstoreConfig::kVolumeThrottlingFieldNumber,
            GetVolumeThrottlingSectionUpdater(),
        },
    };
    return updaters;
}

// Restore the previous message field value and its presence, or clear the
// message field.
void RestoreRejectedSection(
    NProto::TBlockstoreConfig& candidate,
    const NProto::TBlockstoreConfig* previous,
    int fieldNumber)
{
    const auto* field =
        candidate.GetDescriptor()->FindFieldByNumber(fieldNumber);

    Y_ABORT_UNLESS(
        field && field->is_optional() &&
        field->cpp_type() ==
            google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE);

    const auto* reflection = candidate.GetReflection();
    if (previous && reflection->HasField(*previous, field)) {
        reflection->MutableMessage(&candidate, field)
            ->CopyFrom(reflection->GetMessage(*previous, field));
    } else {
        reflection->ClearField(&candidate, field);
    }
}

}   // namespace

bool TConfigSectionsUpdateReport::HasRejectedSections() const
{
    return AnyOf(
        Sections,
        [](const auto& section) { return HasError(section.RejectionError); });
}

TConfigSectionsUpdateReport PrepareDynamicConfigSectionsUpdate(
    NProto::TBlockstoreConfig& dynamicConfigCandidate,
    const NProto::TBlockstoreConfig* previousAcceptedDynamicConfig)
{
    Y_DEBUG_ABORT_UNLESS(
        &dynamicConfigCandidate != previousAcceptedDynamicConfig);

    TConfigSectionsUpdateReport report;

    // Restore each rejected section before preparing the remaining sections.
    for (const auto& entry: GetConfigSectionUpdaters()) {
        TConfigSectionUpdateIssues section;
        section.Path = entry.Path;
        section.RejectionError =
            entry.Updater.Prepare(dynamicConfigCandidate, section.Issues);
        const bool rejected = HasError(section.RejectionError);
        if (rejected) {
            RestoreRejectedSection(
                dynamicConfigCandidate,
                previousAcceptedDynamicConfig,
                entry.FieldNumber);
        }
        if (!section.Issues.empty() || rejected) {
            report.Sections.push_back(std::move(section));
        }
    }
    return report;
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
