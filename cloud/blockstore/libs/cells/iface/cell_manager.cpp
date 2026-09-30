#include "cell_manager.h"

#include <cloud/blockstore/libs/service/context.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCellManagerStub: public ICellManager
{
    const IBlockStorePtr LocalService;

    explicit TCellManagerStub(IBlockStorePtr localService)
        : ICellManager(nullptr)
        , LocalService(std::move(localService))
    {}

    [[nodiscard]] TCellConnectionFuture CreateConnection(
        const TString& cellId,
        const TString& fqdn,
        const NClient::TClientAppConfigPtr& clientConfig,
        ICellConnectionObserverPtr observer) override
    {
        Y_UNUSED(cellId);
        Y_UNUSED(fqdn);
        Y_UNUSED(clientConfig);
        Y_UNUSED(observer);

        return NThreading::MakeFuture(TResultOrError<ICellConnectionPtr>(
            MakeError(E_NOT_IMPLEMENTED, "not implemented")));
    }

    [[nodiscard]] TDescribeVolumeFuture DescribeVolume(
        TCallContextPtr callContext,
        const TString& diskId,
        const NProto::THeaders& headers,
        const NProto::TClientConfig& clientConfig) override
    {
        Y_UNUSED(clientConfig);

        auto req = std::make_shared<NProto::TDescribeVolumeRequest>();
        req->MutableHeaders()->CopyFrom(headers);
        req->SetDiskId(diskId);

        return LocalService->DescribeVolume(
            std::move(callContext),
            std::move(req));
    }

    [[nodiscard]] std::shared_ptr<TCellInboundActivity>
        GetInboundActivity() override
    {
        return nullptr;
    }

    [[nodiscard]] TCellsSnapshot GetSnapshot() override
    {
        return {};
    }

    [[nodiscard]] NThreading::TFuture<TVector<TCellDescribeResult>>
        SearchVolume(TString diskId, TDuration timeout) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(timeout);
        return NThreading::MakeFuture(TVector<TCellDescribeResult>());
    }

    void Start() override
    {}

    void Stop() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ICellManagerPtr CreateCellManagerStub(IBlockStorePtr localService)
{
    return std::make_shared<TCellManagerStub>(std::move(localService));
}

}   // namespace NCloud::NBlockStore::NCells
