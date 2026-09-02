#include "cell_manager.h"

#include <cloud/blockstore/libs/service/context.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCellManagerStub: public ICellManager
{
    explicit TCellManagerStub()
        : ICellManager(nullptr)
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
        IBlockStorePtr service,
        const NProto::TClientConfig& clientConfig) override
    {
        Y_UNUSED(clientConfig);

        auto req = std::make_shared<NProto::TDescribeVolumeRequest>();
        req->MutableHeaders()->CopyFrom(headers);
        req->SetDiskId(diskId);

        return service->DescribeVolume(std::move(callContext), std::move(req));
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
