#pragma once

#include "public.h"

namespace NKikimrFileStore {
    class TConfig;
}

namespace NCloud::NFileStore::NProto {
    class TFileStorePerformanceProfile;
}

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void Convert(
    const NKikimrFileStore::TConfig& config,
    NProto::TFileStorePerformanceProfile& performanceProfile);

void Convert(
    const NProto::TFileStorePerformanceProfile& performanceProfile,
    NKikimrFileStore::TConfig& config);

}   // namespace NCloud::NFileStore::NStorage
