#include "features_config.h"

namespace NCloud::NBlockStore::NStorage {

TFeaturesConfig::TFeaturesConfig(
        NProto::TFeaturesConfig config)
    : Config(std::move(config))
{
    for (const auto& feature: Config.GetFeatures()) {
        TFeatureInfo info;
        info.IsBlacklist = !feature.HasWhitelist();

        const auto& cloudList =
            (feature.HasBlacklist() ? feature.GetBlacklist() : feature.GetWhitelist());

        for (const auto& cloudId: cloudList.GetCloudIds()) {
            info.CloudIds.emplace(cloudId);
        }

        for (const auto& folderId: cloudList.GetFolderIds()) {
            info.FolderIds.emplace(folderId);
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
    const TString& featureName) const
{
    return GetFeature(cloudId, folderId, featureName, nullptr);
}

TString TFeaturesConfig::GetFeatureValue(
    const TString& cloudId,
    const TString& folderId,
    const TString& featureName) const
{
    TString value;
    GetFeature(cloudId, folderId, featureName, &value);
    return value;
}

bool TFeaturesConfig::GetFeature(
    const TString& cloudId,
    const TString& folderId,
    const TString& featureName,
    TString* value) const
{
    bool result = false;

    auto it = Features.find(featureName);
    if (it != Features.end()) {
        auto isBlacklist = it->second.IsBlacklist;
        if (it->second.CloudIds.contains(cloudId)
                || it->second.FolderIds.contains(folderId))
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

}   // namespace NCloud::NBlockStore::NStorage
