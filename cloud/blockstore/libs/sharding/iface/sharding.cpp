#include "sharding.h"

namespace NCloud::NBlockStore::NSharding {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TShardingManagerStub
    : public IShardingManager
{
    explicit TShardingManagerStub()
        : IShardingManager(nullptr)
    {}

    [[nodiscard]] TResultOrError<THostEndpoint> GetShardEndpoint(
        const TString& shardId,
        const NClient::TClientAppConfigPtr& clientConfig) override
    {
        Y_UNUSED(shardId);
        Y_UNUSED(clientConfig);
        return MakeError(E_NOT_IMPLEMENTED, "not implemented");
    }

    [[nodiscard]] std::optional<TDescribeFuture> DescribeVolume(
        const TString& diskId,
        const NProto::THeaders& headers,
        const IBlockStorePtr& localService,
        const NProto::TClientConfig& clientConfig) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(headers);
        Y_UNUSED(localService);
        Y_UNUSED(clientConfig);
        return {};
    }

    void Start() override
    {}

    void Stop() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IShardingManagerPtr CreateShardingManagerStub()
{
    return std::make_shared<TShardingManagerStub>();
}

}   // namespace NCloud::NBlockStore::NSharding
