#include "cells.h"

#include <cloud/blockstore/libs/service/context.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCellsManagerStub
    : public ICellsManager
{
    const bool IsOff;

    explicit TCellsManagerStub(bool isOff)
        : ICellsManager(nullptr)
        , IsOff(isOff)
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
        if (IsOff) {
            return {};
        }
        auto callContext = MakeIntrusive<TCallContext>();

        auto request =
            std::make_shared<NProto::TDescribeVolumeRequest>();
        request->MutableHeaders()->CopyFrom(headers);
        request->SetDiskId(diskId);

        auto future = localService->DescribeVolume(
            callContext,
            std::move(request));

        return future.Apply([] (const auto& future) {
            const auto& result = future.GetValue();
            if (!HasError(result.GetError())) {
                return future;
            }
            NProto::TDescribeVolumeResponse response;
            *response.MutableError() =
                std::move(MakeError(E_REJECTED, "Not all cells available"));
            return NThreading::MakeFuture(std::move(response));
        });
    }

    void Start() override
    {}

    void Stop() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ICellsManagerPtr CreateCellsManagerStub(bool isOff)
{
    return std::make_shared<TCellsManagerStub>(isOff);
}

}   // namespace NCloud::NBlockStore::NCells
