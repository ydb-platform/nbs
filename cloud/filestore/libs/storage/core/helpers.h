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
    class TFileSystemShardCreationState;
    class TUnsafeChangeTabletStateRequest;
    class TUnsafeChangeTabletStateResponse;
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
    [[nodiscard]] bool AddNodeRef(
        const TString& name,
        const TString& shardId,
        const TString& shardNodeName);

    [[nodiscard]] ui32 GetIndex() const;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TProtoRequest>
ui64 CalculateByteCount(const TProtoRequest& request)
{
    return request.GetLength();
}

////////////////////////////////////////////////////////////////////////////////

// Use in diagnostic logs instead of dumping the full shard creation state.
// Includes the version, target range and config hash, but summarizes the bitmap
// as bit count, chunk count and total chunk payload bytes, omitting binary data.
TString DescribeShardCreationState(
    const NProtoPrivate::TFileSystemShardCreationState& state);

// Use when logging UnsafeChangeTabletState requests that may contain shard
// creation state. Includes the filesystem ID and optional control flags, with
// state summarized by DescribeShardCreationState to avoid binary bitmap dumps.
// Falls back to DebugString when the request has no shard creation state.
TString DescribeChangeTabletStateRequest(
    const NProtoPrivate::TUnsafeChangeTabletStateRequest& request);

// Use when logging UnsafeChangeTabletState responses that may contain shard
// creation state. Includes any error and the summary from
// DescribeShardCreationState, omitting binary bitmap data.
// Falls back to ShortUtf8DebugString when the response has no shard creation state.
TString DescribeChangeTabletStateResponse(
    const NProtoPrivate::TUnsafeChangeTabletStateResponse& response);

}   // namespace NCloud::NFileStore::NStorage
