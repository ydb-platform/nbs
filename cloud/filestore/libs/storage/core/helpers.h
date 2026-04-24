#pragma once

#include "public.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

namespace NKikimrFileStore {
    class TConfig;
}   // namespace NKikimrFileStore

namespace NCloud::NFileStore::NProto {
    class TFileStorePerformanceProfile;
    class TListNodesResponse;
}   // namespace NCloud::NFileStore::NProto

namespace NCloud::NFileStore::NProtoPrivate {
    class TListNodesInternalResponse;
}   // namespace NCloud::NFileStore::NProtoPrivate

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void Convert(
    const NKikimrFileStore::TConfig& config,
    NProto::TFileStorePerformanceProfile& performanceProfile);

void Convert(
    const NProto::TFileStorePerformanceProfile& performanceProfile,
    NKikimrFileStore::TConfig& config);

void Convert(
    NProtoPrivate::TListNodesInternalResponse& internalResponse,
    NProto::TListNodesResponse& response);

void Store(
    TStringBuf name,
    TStringBuf shardId,
    TStringBuf shardNodeName,
    ui32 i,
    NProtoPrivate::TListNodesInternalResponse& internalResponse);

////////////////////////////////////////////////////////////////////////////////

template <typename TProtoRequest>
ui64 CalculateByteCount(const TProtoRequest& request)
{
    return request.GetLength();
}

}   // namespace NCloud::NFileStore::NStorage
