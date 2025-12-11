#pragma once

#include "public.h"

namespace NKikimrFileStore {
class TConfig;
}

namespace NCloud::NFileStore::NProto {
class TFileStore;
class TFileStorePerformanceProfile;
}   // namespace NCloud::NFileStore::NProto

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void Convert(
    const NKikimrFileStore::TConfig& config,
    NProto::TFileStore& fileStore);

}   // namespace NCloud::NFileStore::NStorage
