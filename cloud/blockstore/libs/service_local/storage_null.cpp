#include "storage_null.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TNullStorage final
    : public IStorage
{
public:
    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return MakeFuture(NProto::TZeroBlocksResponse());
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return MakeFuture(NProto::TReadBlocksLocalResponse());
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return MakeFuture(NProto::TWriteBlocksLocalResponse());
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);

        return MakeFuture(NProto::TError());
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void ReportIOError() override
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TNullStorageProvider final
    : public IStorageProvider
{
public:
    TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) override
    {
        Y_UNUSED(volume);
        Y_UNUSED(clientId);
        Y_UNUSED(accessMode);

        return MakeFuture<IStoragePtr>(std::make_shared<TNullStorage>());
    };
};

////////////////////////////////////////////////////////////////////////////////

class TControlledStorage final: public IStorage
{
    TFuture<void> WaitForFuture;

public:
    explicit TControlledStorage(TFuture<void> waitFor)
        : WaitForFuture(std::move(waitFor))
    {}

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return WaitForFuture.Apply(
            [](TFuture<void> future)
            {
                Y_UNUSED(future);
                return NProto::TZeroBlocksResponse();
            });
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return WaitForFuture.Apply(
            [](TFuture<void> future)
            {
                Y_UNUSED(future);
                return NProto::TReadBlocksLocalResponse();
            });
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return WaitForFuture.Apply(
            [](TFuture<void> future)
            {
                Y_UNUSED(future);
                return NProto::TWriteBlocksLocalResponse();
            });
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);

        return WaitForFuture.Apply(
            [](TFuture<void> future)
            {
                Y_UNUSED(future);
                return NProto::TError();
            });
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

IStorageProviderPtr CreateNullStorageProvider()
{
    return std::make_shared<TNullStorageProvider>();
}

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateNullStorage()
{
    return std::make_shared<TNullStorage>();
}

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateControlledStorage(TFuture<void> waitFor)
{
    return std::make_shared<TControlledStorage>(std::move(waitFor));
}

}   // namespace NCloud::NBlockStore::NServer
