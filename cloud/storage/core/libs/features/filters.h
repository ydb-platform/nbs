#pragma once

#include <cloud/storage/core/config/features.pb.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NCloud::NFeatures {

////////////////////////////////////////////////////////////////////////////////

class TFilters {
private:
    THashSet<TString> CloudIds;
    THashSet<TString> FolderIds;
    THashSet<TString> EntityIds;   // DiskIds or FsIds

public:
    TFilters(NProto::TFilters config);

    bool Contains(
        const TString& cloudId,
        const TString& folderId,
        const TString& entityId) const;
};

}   // namespace NCloud::NFeatures