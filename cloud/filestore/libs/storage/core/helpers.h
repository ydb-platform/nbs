#pragma once

#include "public.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

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

////////////////////////////////////////////////////////////////////////////////

template <typename TProtoRequest>
ui64 CalculateByteCount(const TProtoRequest& request)
{
    return request.GetLength();
}

}   // namespace NCloud::NFileStore::NStorage
