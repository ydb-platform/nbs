#include "overlapping_requests_guard_service.h"

#include "overlapping_requests_guard.h"
#include "service.h"
#include "service_method.h"

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/system/spinlock.h>

using namespace NThreading;

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TServiceByDiskId = THashMap<TString, IBlockStorePtr>;

// Finds a service by the name of the disk, if it is registered.
class TOverlappingRequestsServicesRegistry
    : public TAtomicRefCount<TOverlappingRequestsServicesRegistry>
{
    const TServiceByDiskId ServiceWrappers;

public:
    explicit TOverlappingRequestsServicesRegistry(
        TServiceByDiskId serviceWrappers)
        : ServiceWrappers(std::move(serviceWrappers))
    {}

    [[nodiscard]] IBlockStorePtr GetService(const TString& diskId) const
    {
        if (const IBlockStorePtr* service = ServiceWrappers.FindPtr(diskId)) {
            return *service;
        }
        return nullptr;
    }

    [[nodiscard]] const TServiceByDiskId& GetServiceWrappers() const
    {
        return ServiceWrappers;
    }
};

using TOverlappingRequestsServicesRegistryPtr =
    TIntrusivePtr<TOverlappingRequestsServicesRegistry>;

////////////////////////////////////////////////////////////////////////////////

class TOverlappingRequestsGuardsService
    : public TBlockStoreImpl<TOverlappingRequestsGuardsService, IBlockStore>
    , public std::enable_shared_from_this<TOverlappingRequestsGuardsService>
{
private:
    const IBlockStorePtr Service;

    // Updating ServicesRegistry should be done under lock to avoid races.
    TAdaptiveLock Lock;
    THotSwap<TOverlappingRequestsServicesRegistry> ServicesRegistry;

public:
    explicit TOverlappingRequestsGuardsService(IBlockStorePtr service)
        : Service(std::move(service))
    {
        TOverlappingRequestsServicesRegistryPtr registry =
            MakeIntrusive<TOverlappingRequestsServicesRegistry>(
                TServiceByDiskId());
        ServicesRegistry.AtomicStore(registry);
    }

    // implements IBlockStore

    void Start() override
    {
        Service->Start();
    }

    void Stop() override
    {
        Service->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Service->AllocateBuffer(bytesCount);
    }

    TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request) override
    {
        return ExecuteWithDiskGuard<NProto::TWriteBlocksResponse>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        return ExecuteWithDiskGuard<NProto::TWriteBlocksLocalResponse>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return ExecuteWithDiskGuard<NProto::TZeroBlocksResponse>(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override
    {
        return ExecuteWithDiskGuard<NProto::TMountVolumeResponse>(
            std::move(callContext),
            std::move(request));
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        return TBlockStoreAdapter::Execute(
            Service.get(),
            std::move(callContext),
            std::move(request));
    }

private:
    template <typename TResponse, typename TRequest>
    TFuture<TResponse> ExecuteWithDiskGuard(
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request)
    {
        const auto& diskId = request->GetDiskId();

        TOverlappingRequestsServicesRegistryPtr servicesRegistry =
            ServicesRegistry.AtomicLoad();
        auto diskService = servicesRegistry->GetService(diskId);
        if (!diskService) {
            diskService = CreateGuard(diskId);
        }

        return TBlockStoreAdapter::Execute(
            diskService.get(),
            std::move(callContext),
            std::move(request));
    }

    IBlockStorePtr CreateGuard(const TString& diskId)
    {
        with_lock (Lock) {
            TOverlappingRequestsServicesRegistryPtr currentServicesRegistry =
                ServicesRegistry.AtomicLoad();

            if (auto diskService = currentServicesRegistry->GetService(diskId))
            {
                // Perhaps someone has already created the necessary handler
                // while we were waiting for the lock.
                return diskService;
            }

            auto guardWrappers = currentServicesRegistry->GetServiceWrappers();
            auto diskService = CreateOverlappingRequestsGuard(Service);
            guardWrappers[diskId] = diskService;

            TOverlappingRequestsServicesRegistryPtr registry =
                MakeIntrusive<TOverlappingRequestsServicesRegistry>(
                    std::move(guardWrappers));
            ServicesRegistry.AtomicStore(registry);

            return diskService;
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateOverlappingRequestsGuardsService(IBlockStorePtr service)
{
    return std::make_shared<TOverlappingRequestsGuardsService>(
        std::move(service));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
