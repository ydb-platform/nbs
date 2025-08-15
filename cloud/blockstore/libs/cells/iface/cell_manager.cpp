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

    [[nodiscard]] TResultOrError<TCellHostEndpoint> GetCellEndpoint(
        const TString& cellId,
        const NClient::TClientAppConfigPtr& clientConfig) override
    {
        Y_UNUSED(cellId);
        Y_UNUSED(clientConfig);
        return MakeError(E_NOT_IMPLEMENTED, "not implemented");
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
