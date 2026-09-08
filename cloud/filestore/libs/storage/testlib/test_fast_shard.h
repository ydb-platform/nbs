#pragma once

#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/vector.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// A shard whose Init the test completes by hand. Serves nothing.
struct TTestFastShard: NFastShard::IFileSystemShard
{
    NThreading::TPromise<NCloud::NProto::TError> InitResult =
        NThreading::NewPromise<NCloud::NProto::TError>();
    bool TornDown = false;

#define FAST_SHARD_NOT_IMPLEMENTED(name, ns, ...)                              \
    NThreading::TFuture<ns::T##name##Response> name(                           \
        ns::T##name##Request request) override                                 \
    {                                                                          \
        return NotImplemented<ns::T##name##Response>(std::move(request));      \
    }                                                                          \
    // FAST_SHARD_NOT_IMPLEMENTED

    FAST_SHARD_PRIVATE_METHODS(FAST_SHARD_NOT_IMPLEMENTED, NProtoPrivate)
    FAST_SHARD_PUBLIC_METHODS(FAST_SHARD_NOT_IMPLEMENTED, NProto)

#undef FAST_SHARD_NOT_IMPLEMENTED

    NThreading::TFuture<NCloud::NProto::TError> Init() override;
    void TearDown() override;
    NThreading::TFuture<NCloud::NProto::TError> CollectStats(
        NFastShard::TFileSystemShardStats* stats) const override;
    void DumpLayoutHtml(IOutputStream& out) const override;
    void DumpLayoutJson(IOutputStream& out) const override;

private:
    template <typename TResponse, typename TRequest>
    static NThreading::TFuture<TResponse> NotImplemented(TRequest request)
    {
        Y_UNUSED(request);

        TResponse response;
        *response.MutableError() = MakeError(E_NOT_IMPLEMENTED);
        return NThreading::MakeFuture(std::move(response));
    }
};

// Every shard the tablet asked for, in order: each boot asks for a new one.
// Goes into TTestEnvConfig::FastShardFactory.
struct TTestFastShards: NFastShard::IFileSystemShardFactory
{
    TVector<std::shared_ptr<TTestFastShard>> Created;

    NFastShard::IFileSystemShardPtr CreateShard(
        const TString& fileSystemId,
        const NProtoPrivate::TFastShardConfig& config,
        ui32 shardNo,
        ui64 generation) override;
};

}   // namespace NCloud::NFileStore::NStorage
