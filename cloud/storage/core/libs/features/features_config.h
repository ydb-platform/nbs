#pragma once

#include "public.h"

#include <cloud/storage/core/config/features.pb.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFeatures {

////////////////////////////////////////////////////////////////////////////////

class TFeaturesConfig
{
    struct TFeatureInfo
    {
        THashSet<TString> CloudIds;
        THashSet<TString> FolderIds;
        THashSet<TString> EntityIds;   // DiskIds or FsIds
        bool IsBlacklist = false;
        double CloudProbability = 0;
        double FolderProbability = 0;
        TString Value;
    };

private:
    const NProto::TFeaturesConfig Config;

    THashMap<TString, TFeatureInfo> Features;

public:
    TFeaturesConfig(NProto::TFeaturesConfig config = {});

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

    TVector<TString> CollectAllFeatures() const;

private:
    bool GetFeature(
        const TString& cloudId,
        const TString& folderId,
        const TString& entityId,
        const TString& featureName,
        TString* value) const;
};

}   // namespace NCloud::NFeatures
