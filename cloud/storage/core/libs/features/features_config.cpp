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

TFeaturesConfig::TFeaturesConfig(
        NProto::TFeaturesConfig config)
    : Config(std::move(config))
{
    for (const auto& feature: Config.GetFeatures()) {
        TFeatureInfo info;
        info.IsBlacklist = feature.HasBlacklist();
        info.CloudProbability = feature.GetCloudProbability();
        info.FolderProbability = feature.GetFolderProbability();

        const auto& cloudList = feature.HasBlacklist()
            ? feature.GetBlacklist() : feature.GetWhitelist();

        for (const auto& cloudId: cloudList.GetCloudIds()) {
            info.CloudIds.emplace(cloudId);
        }

        for (const auto& folderId: cloudList.GetFolderIds()) {
            info.FolderIds.emplace(folderId);
        }

        for (const auto& entityId: cloudList.GetEntityIds()) {
            info.EntityIds.emplace(entityId);
        }

        info.Value = feature.GetValue();

        Features.emplace(feature.GetName(), std::move(info));
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
    bool result = false;

    auto it = Features.find(featureName);
    if (it != Features.end()) {
        auto isBlacklist = it->second.IsBlacklist;
        const bool probabilityMatch =
            ProbabilityMatch(it->second.CloudProbability, cloudId)
            || ProbabilityMatch(it->second.FolderProbability, folderId);

        if (it->second.CloudIds.contains(cloudId)
                || it->second.FolderIds.contains(folderId)
                || it->second.EntityIds.contains(entityId)
                || probabilityMatch)
        {
            result = !isBlacklist;
        } else {
            result = isBlacklist;
        }

        if (result && value) {
            *value = it->second.Value;
        }
    }

    return result;
}

}   // namespace NCloud::NFeatures
