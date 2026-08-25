#pragma once

#include "public.h"

#include "filters.h"

#include <cloud/storage/core/config/features.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NCloud::NFeatures {

////////////////////////////////////////////////////////////////////////////////

class TFeaturesConfig
{
    struct TFeatureInfo
    {
        TFilters Whitelist;
        TFilters Blacklist;
        double CloudProbability = 0;
        double FolderProbability = 0;
        TString Value;

        explicit TFeatureInfo(NProto::TFeatureConfig config);
    };

private:
    // Source feature definitions owned by this object and used to build the
    // lookup map.
    NProto::TFeaturesConfig Config;

    // Parsed feature definitions indexed by feature name.
    THashMap<TString, TFeatureInfo> Features;

public:
    explicit TFeaturesConfig(NProto::TFeaturesConfig config = {});

    [[nodiscard]] const NProto::TFeaturesConfig& GetConfigProto() const;

    bool IsValid() const;

    bool IsFeatureEnabled(
        const TString& cloudId,
        const TString& folderId,
        const TString& entityId,
        const TString& featureName) const;

    TString GetFeatureValue(
        const TString& cloudId,
        const TString& folderId,
        const TString& entityId,
        const TString& featureName) const;

    template <typename T>
    bool TryGetFeatureValue(
        const TString& cloudId,
        const TString& folderId,
        const TString& entityId,
        const TString& featureName,
        T& value,
        TString& errorMessage) const;

    TVector<TString> CollectAllFeatures() const;

private:
    bool GetFeature(
        const TString& cloudId,
        const TString& folderId,
        const TString& entityId,
        const TString& featureName,
        TString* value) const;
};

template <typename T>
bool TFeaturesConfig::TryGetFeatureValue(
    const TString& cloudId,
    const TString& folderId,
    const TString& entityId,
    const TString& featureName,
    T& value,
    TString& errorMessage) const
{
    TString featureValue;
    if (!GetFeature(cloudId, folderId, entityId, featureName, &featureValue)) {
        // It's not an error if a feature is not found.
        return false;
    }

    if constexpr (std::is_same_v<T, bool>) {
        // For backward compatibility, an empty feature value means `true`.
        if (featureValue.empty()) {
            value = true;
            return true;
        }
    }

    if (!TryFromString(featureValue, value)) {
        errorMessage += TStringBuilder()
                        << "Value '" << featureValue << "' of feature '"
                        << featureName
                        << "' cannot be converted to the target field type. ";
        return false;
    }

    return true;
}

}   // namespace NCloud::NFeatures
