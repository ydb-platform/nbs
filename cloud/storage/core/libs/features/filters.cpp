#include "filters.h"

#include <util/generic/algorithm.h>

#include <regex>

namespace NCloud::NFeatures {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool RegexpMatchItemInSet(const THashSet<TString>& set, const TString& item)
{
    return AnyOf(
        set,
        [&](const TString& setElem)
        {
            std::regex r{setElem.data(), setElem.size()};
            return std::regex_match(item.data(), r);
        });
}

}  // namespace

////////////////////////////////////////////////////////////////////////////////

TFilters::TFilters(NProto::TFilters config)
{
    for (const auto& cloudId: config.GetCloudIds()) {
        CloudIds.emplace(cloudId);
    }
    for (const auto& folderId: config.GetFolderIds()) {
        FolderIds.emplace(folderId);
    }
    for (const auto& entityId: config.GetEntityIds()) {
        EntityIds.emplace(entityId);
    }

    MatchAlgorithm = config.GetMatchAlgorithm();
}

bool TFilters::Contains(
    const TString& cloudId,
    const TString& folderId,
    const TString& entityId) const
{
    switch (MatchAlgorithm) {
        case NProto::FILTER_MATCH_ALGORITHM_STRING:
            return CloudIds.contains(cloudId) || FolderIds.contains(folderId) ||
                   EntityIds.contains(entityId);

        case NProto::FILTER_MATCH_ALGORITHM_REGEXP:
            return RegexpMatchItemInSet(CloudIds, cloudId) ||
                   RegexpMatchItemInSet(FolderIds, folderId) ||
                   RegexpMatchItemInSet(EntityIds, entityId);
    }
}

}   // namespace NCloud::NFeatures
