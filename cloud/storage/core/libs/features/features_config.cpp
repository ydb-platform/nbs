#include "features_config.h"

#include <util/digest/city.h>

namespace NCloud::NFeatures {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool ProbabilityMatch(double p, const TString& s)
{
    return p && CityHash64(s) / static_cast<double>(Max<ui64>()) < p;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFeaturesConfig::TFeatureInfo::TFeatureInfo(NProto::TFeatureConfig config)
    : Whitelist(config.GetWhitelist())
    , Blacklist(config.GetBlacklist())
    , Value(config.GetValue())
{
    double defaultProbability =
        config.HasBlacklist() && !config.HasWhitelist() ? 1.0 : 0.0;

    CloudProbability = config.HasCloudProbability()
                           ? config.GetCloudProbability()
                           : defaultProbability;

    FolderProbability = config.HasFolderProbability()
                            ? config.GetFolderProbability()
                            : defaultProbability;
}

TFeaturesConfig::TFeaturesConfig(NProto::TFeaturesConfig config)
    : Config(std::move(config))
{
    for (const auto& feature: Config.GetFeatures()) {
        Features.emplace(feature.GetName(), feature);
    }
}

bool TFeaturesConfig::IsValid() const
{
    return true;
}

bool TFeaturesConfig::IsFeatureEnabled(
    const TString& cloudId,
    const TString& folderId,
    const TString& entityId,
    const TString& featureName) const
{
    return GetFeature(cloudId, folderId, entityId, featureName, nullptr);
}

TString TFeaturesConfig::GetFeatureValue(
    const TString& cloudId,
    const TString& folderId,
    const TString& entityId,
    const TString& featureName) const
{
    TString value;
    GetFeature(cloudId, folderId, entityId, featureName, &value);
    return value;
}

TVector<TString> TFeaturesConfig::CollectAllFeatures() const
{
    TVector<TString> result;
    for (const auto& x: Features) {
        result.push_back(x.first);
    }
    return result;
}

bool TFeaturesConfig::GetFeature(
    const TString& cloudId,
    const TString& folderId,
    const TString& entityId,
    const TString& featureName,
    TString* value) const
{
    auto it = Features.find(featureName);

    if (it != Features.end()) {
        auto& feature = it->second;

        if (feature.Blacklist.Contains(cloudId, folderId, entityId)) {
            return false;
        }

        if (feature.Whitelist.Contains(cloudId, folderId, entityId) ||
            ProbabilityMatch(feature.CloudProbability, cloudId) ||
            ProbabilityMatch(feature.FolderProbability, folderId))
        {
            if (value) {
                *value = feature.Value;
            }
            return true;
        }
    }

    return false;
}

}   // namespace NCloud::NFeatures
