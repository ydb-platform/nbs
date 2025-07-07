#include "cells.h"

#include <cloud/blockstore/libs/service/context.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCellsManagerStub
    : public ICellsManager
{
    explicit TCellsManagerStub()
        : ICellsManager(nullptr)
    {}

    [[nodiscard]] TResultOrError<THostEndpoint> GetCellEndpoint(
        const TString& cellId,
        const NClient::TClientAppConfigPtr& clientConfig) override
    {
        Y_UNUSED(cellId);
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

ICellsManagerPtr CreateCellsManagerStub()
{
    return std::make_shared<TCellsManagerStub>();
}

}   // namespace NCloud::NBlockStore::NCells
