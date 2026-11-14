/*******************************************************************************

  * Feature composition keeps the first definition of each name within a source.

  * Dynamic definitions replace complete static records with the same name
    at their positions.

  * New names follow the static records in dynamic source order.

  * Other fields retain the preceding protobuf merge result.

*******************************************************************************/

#include "features.h"

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/compiler.h>

namespace NCloud::NBlockStore {

namespace {

class TFeaturesSectionUpdater final: public IConfigSectionUpdater
{
public:
    [[nodiscard]] NCloud::NProto::TError Prepare(
        NProto::TBlockstoreConfig& dynamicConfigCandidate,
        const NProto::TBlockstoreConfig* previousAcceptedDynamicConfig,
        TVector<TString>& issues) const override;

    void Merge(
        const NProto::TBlockstoreConfig& staticConfig,
        const NProto::TBlockstoreConfig& dynamicConfig,
        NProto::TBlockstoreConfig& result) const override;
};

// Merge feature definitions by name so an overridden feature has one runtime
// record. Preserve the existing first-wins behavior within each source layer.
void MergeFeatures(
    const NCloud::NProto::TFeaturesConfig& staticFeatures,
    const NCloud::NProto::TFeaturesConfig& dynamicFeatures,
    NCloud::NProto::TFeaturesConfig* result)
{
    // TFeaturesConfig uses the first record when feature names are duplicated.
    // Preserve this behavior while canonicalizing the static layer.
    TVector<NCloud::NProto::TFeatureConfig> features;
    features.reserve(
        staticFeatures.FeaturesSize() + dynamicFeatures.FeaturesSize());

    THashMap<TString, size_t> featureIndices;
    for (const auto& feature: staticFeatures.GetFeatures()) {
        const bool inserted =
            featureIndices.emplace(feature.GetName(), features.size()).second;
        if (inserted) {
            features.push_back(feature);
        }
    }

    // Ignore duplicate names after the first one in the dynamic layer. The
    // first dynamic record replaces the static record or is appended if none
    // exists.
    THashSet<TString> dynamicFeatureNames;
    for (const auto& feature: dynamicFeatures.GetFeatures()) {
        if (!dynamicFeatureNames.insert(feature.GetName()).second) {
            continue;
        }

        const auto it = featureIndices.find(feature.GetName());
        if (it != featureIndices.end()) {
            // Replace the complete static record at its original position, so
            // fields omitted from the dynamic record do not leak from static.
            features[it->second].CopyFrom(feature);
        } else {
            // Append dynamic-only records in their source order.
            features.push_back(feature);
        }
    }

    // Rebuild only the repeated Features field after the enclosing Features
    // message has been merged, preserving its other and unknown fields.
    result->ClearFeatures();
    for (const auto& feature: features) {
        result->AddFeatures()->CopyFrom(feature);
    }
}

// Accept the Features section unchanged because it needs no preparation.
NCloud::NProto::TError TFeaturesSectionUpdater::Prepare(
    NProto::TBlockstoreConfig& dynamicConfigCandidate,
    const NProto::TBlockstoreConfig* previousAcceptedDynamicConfig,
    TVector<TString>& issues) const
{
    Y_UNUSED(dynamicConfigCandidate);
    Y_UNUSED(previousAcceptedDynamicConfig);
    issues.clear();

    return {};
}

// Replace named features while preserving their static record positions.
void TFeaturesSectionUpdater::Merge(
    const NProto::TBlockstoreConfig& staticConfig,
    const NProto::TBlockstoreConfig& dynamicConfig,
    NProto::TBlockstoreConfig& result) const
{
    if (staticConfig.GetFeatures().FeaturesSize() ||
        dynamicConfig.GetFeatures().FeaturesSize())
    {
        MergeFeatures(
            staticConfig.GetFeatures(),
            dynamicConfig.GetFeatures(),
            result.MutableFeatures());
    }
}

}   // namespace

const IConfigSectionUpdater& GetFeaturesSectionUpdater()
{
    static const TFeaturesSectionUpdater updater;
    return updater;
}

}   // namespace NCloud::NBlockStore
