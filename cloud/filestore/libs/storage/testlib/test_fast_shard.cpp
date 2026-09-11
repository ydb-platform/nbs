#include "test_fast_shard.h"

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<NCloud::NProto::TError> TTestFastShard::Init()
{
    return InitResult.GetFuture();
}

void TTestFastShard::TearDown()
{
    TornDown = true;
}

NThreading::TFuture<NCloud::NProto::TError> TTestFastShard::CollectStats(
    NFastShard::TFileSystemShardStats* stats) const
{
    Y_UNUSED(stats);

    return NThreading::MakeFuture(MakeError(E_NOT_IMPLEMENTED));
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
    Y_UNUSED(fileSystemId, config, shardNo);

    Created.push_back(std::make_shared<TTestFastShard>());
    Created.back()->Generation = generation;
    return Created.back();
}

}   // namespace NCloud::NFileStore::NStorage
