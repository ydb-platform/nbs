#include "remote_storage.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/storage.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NCells {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

// A gRPC server fills Headers.Internal in for itself and a server in another
// cell refuses it; nothing on this side reads it once the request leaves.
struct TRemoteStorage: public IStorage
{
    const IBlockStorePtr Endpoint;
    const ICellConnectionPtr Connection;

    TRemoteStorage(IBlockStorePtr endpoint, ICellConnectionPtr connection)
        : Endpoint(std::move(endpoint))
        , Connection(std::move(connection))
    {}

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        request->MutableHeaders()->ClearInternal();
        return Endpoint->ZeroBlocks(std::move(callContext), std::move(request));
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        request->MutableHeaders()->ClearInternal();
        return Endpoint->ReadBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        request->MutableHeaders()->ClearInternal();
        return Endpoint->WriteBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);
        return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void ReportIOError() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateRemoteStorage(
    IBlockStorePtr endpoint,
    ICellConnectionPtr connection)
{
    return std::make_shared<TRemoteStorage>(
        std::move(endpoint),
        std::move(connection));
}

}   // namespace NCloud::NBlockStore::NCells
