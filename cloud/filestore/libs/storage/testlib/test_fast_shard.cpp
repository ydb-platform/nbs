#include "test_fast_shard.h"

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using namespace NThreading;

TFuture<NProtoPrivate::TCreateSessionResponse> TTestFastShard::CreateSession(
    NProtoPrivate::TCreateSessionRequest request)
{
    Y_UNUSED(request);
    return MakeFuture(NProtoPrivate::TCreateSessionResponse{});
}

TFuture<NProto::TResetSessionResponse> TTestFastShard::ResetSession(
    NProto::TResetSessionRequest request)
{
    Y_UNUSED(request);
    return MakeFuture(NProto::TResetSessionResponse{});
}

TFuture<NProtoPrivate::TDestroySessionResponse> TTestFastShard::DestroySession(
    NProtoPrivate::TDestroySessionRequest request)
{
    Y_UNUSED(request);
    return MakeFuture(NProtoPrivate::TDestroySessionResponse{});
}

TFuture<NCloud::NProto::TError> TTestFastShard::Init()
{
    return InitResult.GetFuture();
}

TFuture<NCloud::NProto::TError> TTestFastShard::Format()
{
    return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
}

void TTestFastShard::TearDown()
{
    TornDown = true;
}

TFuture<NCloud::NProto::TError> TTestFastShard::CollectStats(
    NFastShard::TFileSystemShardStats* stats) const
{
    Y_UNUSED(stats);

    return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
}

void TTestFastShard::DumpLayoutHtml(IOutputStream& out) const
{
    Y_UNUSED(out);
}

void TTestFastShard::DumpLayoutJson(IOutputStream& out) const
{
    Y_UNUSED(out);
}

////////////////////////////////////////////////////////////////////////////////

NFastShard::IFileSystemShardPtr TTestFastShards::CreateShard(
    const TString& fileSystemId,
    const NProtoPrivate::TFastShardConfig& config,
    ui32 shardNo,
    ui64 generation)
{
    Y_UNUSED(fileSystemId, shardNo);

    Created.push_back(std::make_shared<TTestFastShard>());
    Created.back()->Generation = generation;
    Created.back()->Config = config;
    return Created.back();
}

}   // namespace NCloud::NFileStore::NStorage
