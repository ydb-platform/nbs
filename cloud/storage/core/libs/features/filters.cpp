#include "filters.h"

namespace NCloud::NFeatures {

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
}

bool TFilters::Contains(
    const TString& cloudId,
    const TString& folderId,
    const TString& entityId) const
{
    return CloudIds.contains(cloudId) || FolderIds.contains(folderId) ||
        EntityIds.contains(entityId);
}

}   // namespace NCloud::NFeatures