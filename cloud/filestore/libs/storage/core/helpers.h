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

////////////////////////////////////////////////////////////////////////////////

void Convert(
    NProtoPrivate::TListNodesInternalResponse& internalResponse,
    NProto::TListNodesResponse& response);

////////////////////////////////////////////////////////////////////////////////

class TListNodesInternalResponseBuilder
{
private:
    struct TImpl;
    THolder<TImpl> Impl;

public:
    TListNodesInternalResponseBuilder(
        NProtoPrivate::TListNodesInternalResponse& response,
        ui64 nameBufferSize,
        ui64 externalRefBufferSize,
        ui64 nameCount,
        ui64 externalRefCount);

    ~TListNodesInternalResponseBuilder();

public:
    void AddNodeRef(
        TStringBuf name,
        TStringBuf shardId,
        TStringBuf shardNodeName);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TProtoRequest>
ui64 CalculateByteCount(const TProtoRequest& request)
{
    return request.GetLength();
}

}   // namespace NCloud::NFileStore::NStorage
