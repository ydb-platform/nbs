#pragma once

#include "public.h"

#include <cloud/blockstore/config/features.pb.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TFeaturesConfig
{
    struct TFeatureInfo
    {
        THashSet<TString> CloudIds;
        THashSet<TString> FolderIds;
        bool IsBlacklist;
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
        const TString& featureName) const;

    TString GetFeatureValue(
        const TString& cloudId,
        const TString& folderId,
        const TString& featureName) const;

private:
    bool GetFeature(
        const TString& cloudId,
        const TString& folderId,
        const TString& featureName,
        TString* value) const;
};

}   // namespace NCloud::NBlockStore::NStorage
