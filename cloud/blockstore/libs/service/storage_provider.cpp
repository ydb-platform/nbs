#include "storage_provider.h"

#include "context.h"
#include "service.h"
#include "storage.h"

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TServiceStorage final: public IStorage
{
private:
    const IBlockStorePtr Service;
    const TString DiskId;

public:
    TServiceStorage(IBlockStorePtr service, TString diskId)
        : Service(std::move(service))
        , DiskId(std::move(diskId))
    {}

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        PrepareRequest(*request);
        return Service->ZeroBlocks(std::move(callContext), std::move(request));
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        PrepareRequest(*request);
        return Service->ReadBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        PrepareRequest(*request);
        return Service->WriteBlocksLocal(
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

private:
    template <typename T>
    void PrepareRequest(T& request)
    {
        // TODO: add diskId check
        request.SetDiskId(DiskId);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDefaultStorageProvider final: public IStorageProvider
{
private:
    IBlockStorePtr Service;

public:
    TDefaultStorageProvider(IBlockStorePtr service)
        : Service(std::move(service))
    {}

    TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) override
    {
        Y_UNUSED(clientId);
        Y_UNUSED(accessMode);

        auto storage =
            std::make_shared<TServiceStorage>(Service, volume.GetDiskId());

        return MakeFuture<IStoragePtr>(storage);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMultiStorageProvider
    : public IStorageProvider
    , public std::enable_shared_from_this<TMultiStorageProvider>
{
private:
    TVector<IStorageProviderPtr> StorageProviders;

public:
    TMultiStorageProvider(TVector<IStorageProviderPtr> storageProviders)
        : StorageProviders(std::move(storageProviders))
    {}

    TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) override
    {
        return CreateStorage(0, volume, clientId, accessMode);
    }

private:
    TFuture<IStoragePtr> CreateStorage(
        size_t storageNum,
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode)
    {
        if (storageNum >= StorageProviders.size()) {
            return MakeFuture<IStoragePtr>(nullptr);
        }

        auto weak_ptr = weak_from_this();

        const auto& storageProvider = StorageProviders[storageNum];
        return storageProvider->CreateStorage(volume, clientId, accessMode)
            .Apply(
                [=, weak_ptr = std::move(weak_ptr)](const auto& future)
                {
                    const auto& storage = future.GetValue();
                    if (storage) {
                        return MakeFuture(storage);
                    }

                    if (auto p = weak_ptr.lock()) {
                        return p->CreateStorage(
                            storageNum + 1,
                            volume,
                            clientId,
                            accessMode);
                    }

                    return MakeFuture<IStoragePtr>(nullptr);
                });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateDefaultStorageProvider(IBlockStorePtr service)
{
    return std::make_shared<TDefaultStorageProvider>(std::move(service));
}

IStorageProviderPtr CreateMultiStorageProvider(
    TVector<IStorageProviderPtr> storageProviders)
{
    return std::make_shared<TMultiStorageProvider>(std::move(storageProviders));
}

}   // namespace NCloud::NBlockStore
