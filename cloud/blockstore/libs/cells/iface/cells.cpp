#include "cells.h"

#include <cloud/blockstore/libs/service/context.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCellManagerStub
    : public ICellManager
{
    explicit TCellManagerStub()
        : ICellManager(nullptr)
    {}

    [[nodiscard]] TResultOrError<TCellHostEndpoint> GetCellEndpoint(
        const TString& cellId,
        const NClient::TClientAppConfigPtr& clientConfig) override
    {
        Y_UNUSED(cellId);
        Y_UNUSED(clientConfig);
        return MakeError(E_NOT_IMPLEMENTED, "not implemented");
    }

    [[nodiscard]] NThreading::TFuture<NProto::TDescribeVolumeResponse>
    DescribeVolume(
        TCallContextPtr callContext,
        const TString& diskId,
        const NProto::THeaders& headers,
        IBlockStorePtr localService,
        const NProto::TClientConfig& clientConfig) override
    {
        Y_UNUSED(clientConfig);

        auto describeRequest =
            std::make_shared<NProto::TDescribeVolumeRequest>();
        describeRequest->MutableHeaders()->CopyFrom(headers);
        describeRequest->SetDiskId(diskId);

        return localService->DescribeVolume(
            std::move(callContext),
            std::move(describeRequest));
    }

    void Start() override
    {}

    void Stop() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ICellManagerPtr CreateCellManagerStub()
{
    return std::make_shared<TCellManagerStub>();
}

}   // namespace NCloud::NBlockStore::NCells
