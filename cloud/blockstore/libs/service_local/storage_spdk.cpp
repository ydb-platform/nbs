#include "storage_spdk.h"

#include "compound_storage.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/common/caching_allocator.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/spdk/iface/device.h>
#include <cloud/blockstore/libs/spdk/iface/env.h>
#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <library/cpp/deprecated/atomic/atomic.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;
using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse>
TFuture<TResponse> FutureErrorResponse(ui32 code, TString message)
{
    return MakeFuture(ErrorResponse<TResponse>(code, std::move(message)));
}

////////////////////////////////////////////////////////////////////////////////

class TSpdkStorage
    : public IStorage
{
private:
    NSpdk::ISpdkDevicePtr Device;
    ICachingAllocatorPtr Allocator;
    const ui32 BlockSize;

public:
    TSpdkStorage(
            NSpdk::ISpdkDevicePtr device,
            ICachingAllocatorPtr allocator,
            ui32 blockSize)
        : Device(std::move(device))
        , Allocator(std::move(allocator))
        , BlockSize(blockSize)
    {}

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override;

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override;

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override;

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override;

    TStorageBuffer AllocateBuffer(size_t bytesCount) override;

    void ReportIOError() override;
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TReadBlocksLocalResponse> TSpdkStorage::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
{
    Y_UNUSED(callContext);

    const ui64 startIndex = request->GetStartIndex();
    const ui32 totalBlockCount = request->GetBlocksCount();

    // std::function can't work with move only objects
    auto guard = std::make_shared<TGuardedSgList::TGuard>(
        request->Sglist.Acquire());

    if (!*guard) {
        return FutureErrorResponse<NProto::TReadBlocksLocalResponse>(
            E_CANCELLED,
            "failed to acquire sglist in SpdkStorage");
    }

    auto result = Device->Read(
        guard->Get(),
        startIndex * BlockSize,
        totalBlockCount * BlockSize);

    return result.Apply([request, guard] (const auto& future) mutable {
        guard.reset();

        NProto::TReadBlocksLocalResponse response;
        *response.MutableError() = future.GetValue();

        return response;
    });
}

TFuture<NProto::TWriteBlocksLocalResponse> TSpdkStorage::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    Y_UNUSED(callContext);

    const ui64 startIndex = request->GetStartIndex();
    const ui32 totalBlockCount = request->BlocksCount;

    // std::function can't work with move only objects
    auto guard = std::make_shared<TGuardedSgList::TGuard>(
        request->Sglist.Acquire());

    if (!*guard) {
        return FutureErrorResponse<NProto::TWriteBlocksLocalResponse>(
            E_CANCELLED,
            "failed to acquire sglist in SpdkStorage");
    }

    auto result = Device->Write(
        guard->Get(),
        startIndex * BlockSize,
        totalBlockCount * BlockSize);

    return result.Apply([request, guard] (const auto& future) mutable {
        guard.reset();

        NProto::TWriteBlocksLocalResponse response;
        *response.MutableError() = future.GetValue();

        return response;
    });
}

TFuture<NProto::TZeroBlocksResponse> TSpdkStorage::ZeroBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    Y_UNUSED(callContext);

    const ui64 startIndex = request->GetStartIndex();
    const ui32 totalBlockCount = request->GetBlocksCount();

    auto result = Device->WriteZeroes(
        startIndex * BlockSize,
        totalBlockCount * BlockSize);

    return result.Apply([] (const auto& future) {
        NProto::TZeroBlocksResponse response;
        *response.MutableError() = future.GetValue();

        return response;
    });
}

TFuture<NProto::TError> TSpdkStorage::EraseDevice(
    NProto::EDeviceEraseMethod method)
{
    return Device->Erase(method);
}

TStorageBuffer TSpdkStorage::AllocateBuffer(size_t bytesCount)
{
    size_t space = bytesCount + BlockSize;

    auto alloc = Allocator;
    auto block = alloc->Allocate(space);
    void* p = block.Data;

    Y_ABORT_UNLESS(std::align(BlockSize, bytesCount, p, space));

    return { static_cast<char*>(p), [=] (auto*) {
        alloc->Release(block);
    }};
}

void TSpdkStorage::ReportIOError()
{}

////////////////////////////////////////////////////////////////////////////////

class TReadOnlySpdkStorage final
    : public TSpdkStorage
{
public:
    using TSpdkStorage::TSpdkStorage;

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return FutureErrorResponse<NProto::TZeroBlocksResponse>(
            E_IO,
            "Volume in error state"
        );
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return FutureErrorResponse<NProto::TWriteBlocksLocalResponse>(
            E_IO,
            "Volume in error state"
        );
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRegistrationContext
{
    TVector<IStoragePtr> Devices;
    TVector<ui64> Offsets;
    const ui32 BlockSize;
    TString DiskId;
    TString ClientId;

    TAtomic RemainingRequests;

    TString LastError;
    TAdaptiveLock Lock;

    TPromise<IStoragePtr> Promise;

    IServerStatsPtr ServerStats;

    ICachingAllocatorPtr Allocator;
    const bool ReadOnly;

    TRegistrationContext(
            ui32 deviceCount,
            ui32 blockSize,
            TString diskId,
            TString clientId,
            IServerStatsPtr serverStats,
            ICachingAllocatorPtr allocator,
            bool readOnly)
        : Devices(deviceCount)
        , Offsets(deviceCount)
        , BlockSize(blockSize)
        , DiskId(std::move(diskId))
        , ClientId(std::move(clientId))
        , RemainingRequests(deviceCount)
        , Promise(NThreading::NewPromise<IStoragePtr>())
        , ServerStats(std::move(serverStats))
        , Allocator(std::move(allocator))
        , ReadOnly(readOnly)
    {}

    void OnResponse(ui32 i, NSpdk::ISpdkDevicePtr device)
    {
        Devices[i] = ReadOnly
            ? CreateReadOnlySpdkStorage(std::move(device), Allocator, BlockSize)
            : CreateSpdkStorage(std::move(device), Allocator, BlockSize);

        if (AtomicDecrement(RemainingRequests) == 0) {
            if (LastError) {
                Promise.SetException(LastError);
            } else {
                Promise.SetValue(CreateCompoundStorage(
                    std::move(Devices),
                    std::move(Offsets),
                    BlockSize,
                    std::move(DiskId),
                    std::move(ClientId),
                    std::move(ServerStats)));
            }
        }
    }

    void OnError(TString error)
    {
        with_lock (Lock) {
            LastError = error;
        }

        if (AtomicDecrement(RemainingRequests) == 0) {
            Promise.SetException(LastError);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSpdkStorageProvider final
    : public IStorageProvider
{
private:
    const NSpdk::ISpdkEnvPtr Spdk;
    const ICachingAllocatorPtr Allocator;
    const IServerStatsPtr ServerStats;

public:
    TSpdkStorageProvider(
            NSpdk::ISpdkEnvPtr spdk,
            ICachingAllocatorPtr allocator,
            IServerStatsPtr serverStats)
        : Spdk(std::move(spdk))
        , Allocator(std::move(allocator))
        , ServerStats(std::move(serverStats))
    {}

    TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) override
    {
        const auto kind = volume.GetStorageMediaKind();
        if (kind != NProto::STORAGE_MEDIA_SSD_NONREPLICATED) {
            return MakeFuture<IStoragePtr>(nullptr);
        }

        const auto& devices = volume.GetDevices();
        for (const auto& device: devices) {
            if (device.GetTransportId().empty()) {
                // agent doesn't support nvme-of
                return MakeFuture<IStoragePtr>(nullptr);
            }
        }

        const bool write = IsReadWriteMode(accessMode);
        const bool readOnly = volume.GetIOMode() != NProto::VOLUME_IO_OK;

        auto registrationContext = std::make_shared<TRegistrationContext>(
            devices.size(),
            volume.GetBlockSize(),
            volume.GetDiskId(),
            clientId,
            ServerStats,
            Allocator,
            readOnly);

        ui64 offset = 0;
        for (int i = 0; i < devices.size(); ++i) {
            offset += devices[i].GetBlockCount();
            registrationContext->Offsets[i] = offset;
        }

        for (int i = 0; i < devices.size(); ++i) {
            Spdk->RegisterNVMeDevices(devices[i].GetBaseName(),
                                      devices[i].GetTransportId())
                .Subscribe([=, this] (const auto& future) {
                    try {
                        const auto& remoteDevices = future.GetValue();
                        Y_ENSURE(remoteDevices.size() == 1);
                        Spdk->OpenDevice(remoteDevices[0], write).Subscribe(
                            [=] (const auto& future) {
                                try {
                                    auto device = future.GetValue();
                                    registrationContext->OnResponse(
                                        i,
                                        std::move(device)
                                    );
                                } catch (const yexception& e) {
                                    registrationContext->OnError(e.what());
                                }
                            }
                        );
                    } catch (const yexception& e) {
                        registrationContext->OnError(e.what());
                    }
                }
            );
        }

        return registrationContext->Promise;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateSpdkStorageProvider(
    NSpdk::ISpdkEnvPtr spdk,
    ICachingAllocatorPtr allocator,
    IServerStatsPtr serverStats)
{
    return std::make_shared<TSpdkStorageProvider>(
        std::move(spdk),
        std::move(allocator),
        std::move(serverStats));
}

IStoragePtr CreateSpdkStorage(
    NSpdk::ISpdkDevicePtr device,
    ICachingAllocatorPtr allocator,
    ui32 blockSize)
{
    return std::make_shared<TSpdkStorage>(
        std::move(device),
        std::move(allocator),
        blockSize);
}

IStoragePtr CreateReadOnlySpdkStorage(
    NSpdk::ISpdkDevicePtr device,
    ICachingAllocatorPtr allocator,
    ui32 blockSize)
{
    return std::make_shared<TReadOnlySpdkStorage>(
        std::move(device),
        std::move(allocator),
        blockSize);
}

}   // namespace NCloud::NBlockStore::NServer
