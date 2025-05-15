#include "spdk_client.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/spdk/iface/device.h>
#include <cloud/blockstore/libs/spdk/iface/env.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

using namespace NCloud::NBlockStore::NSpdk;

namespace {

///////////////////////////////////////////////////////////////////////////////

static constexpr TDuration WaitTimeout = TDuration::Seconds(30);

static const TString IQN_CLIENT = "iqn.2016-06.io.spdk:client";

///////////////////////////////////////////////////////////////////////////////

class TEndpointBase
    : public IBlockStore
{
public:
    TEndpointBase() = default;

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        Y_UNUSED(request);                                                     \
        const auto& type = GetBlockStoreRequestName(EBlockStoreRequest::name); \
        return MakeFuture<NProto::T##name##Response>(                          \
            TErrorResponse(E_NOT_IMPLEMENTED, TStringBuilder()                 \
                << "Unsupported request " << type.Quote()));                   \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

class TEndpoint final
    : public TEndpointBase
{
private:
    const ISpdkEnvPtr Env;
    const ISpdkDevicePtr Device;
    const IBlockStorePtr VolumeClient;
    const ui32 BlockSize;

    IAllocator* Allocator;

public:
    TEndpoint(
            ISpdkEnvPtr env,
            ISpdkDevicePtr device,
            IBlockStorePtr volumeClient,
            ui32 blockSize)
        : Env(std::move(env))
        , Device(std::move(device))
        , VolumeClient(std::move(volumeClient))
        , BlockSize(blockSize)
    {
        Allocator = Env->GetAllocator();
    }

    ~TEndpoint()
    {
        Stop();
    }

    void Start() override;
    void Stop() override;

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override;

    TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TUnmountVolumeRequest> request) override;

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override;

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override;

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override;

    TStorageBuffer AllocateBuffer(size_t bytesCount) override;
};

////////////////////////////////////////////////////////////////////////////////

void TEndpoint::Start()
{
    Device->Start();
}

void TEndpoint::Stop()
{
    Device->Stop();
}

TFuture<NProto::TMountVolumeResponse> TEndpoint::MountVolume(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TMountVolumeRequest> request)
{
    // TODO
    return VolumeClient->MountVolume(
        std::move(callContext),
        std::move(request));
}

TFuture<NProto::TUnmountVolumeResponse> TEndpoint::UnmountVolume(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TUnmountVolumeRequest> request)
{
    // TODO
    return VolumeClient->UnmountVolume(
        std::move(callContext),
        std::move(request));
}

TFuture<NProto::TReadBlocksLocalResponse> TEndpoint::ReadBlocksLocal(
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
        return MakeFuture<NProto::TReadBlocksLocalResponse>(TErrorResponse(
            E_CANCELLED, "failed to acquire sglist in SpdkClient"));
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

TFuture<NProto::TWriteBlocksLocalResponse> TEndpoint::WriteBlocksLocal(
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
        return MakeFuture<NProto::TWriteBlocksLocalResponse>(TErrorResponse(
            E_CANCELLED,
            "failed to acquire sglist in SpdkClient"));
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

TFuture<NProto::TZeroBlocksResponse> TEndpoint::ZeroBlocks(
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

TStorageBuffer TEndpoint::AllocateBuffer(size_t bytesCount)
{
    size_t space = bytesCount + BlockSize;

    auto block = Allocator->Allocate(space);
    void* p = block.Data;

    Y_ABORT_UNLESS(std::align(BlockSize, bytesCount, p, space));

    return { static_cast<char*>(p), [=, this] (auto*) {
        Allocator->Release(block);
    }};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateSpdkEndpointClient(
    NSpdk::ISpdkEnvPtr env,
    IBlockStorePtr volumeClient,
    const TString& deviceName)
{
    auto stats = env->QueryDeviceStats(deviceName)
        .GetValue(WaitTimeout);

    auto device = env->OpenDevice(deviceName, true)
        .GetValue(WaitTimeout);

    return std::make_shared<TEndpoint>(
        std::move(env),
        std::move(device),
        std::move(volumeClient),
        stats.BlockSize);
}

IBlockStorePtr CreateNVMeEndpointClient(
    NSpdk::ISpdkEnvPtr env,
    IBlockStorePtr volumeClient,
    const TNVMeEndpointConfig& config)
{
    auto transportId = config.DeviceTransportId;
    if (config.DeviceNqn) {
        transportId += " subnqn:" + config.DeviceNqn + config.SocketPath;
    }

    auto future = env->RegisterNVMeDevices("nvme", transportId);

    auto devices = future.GetValue(WaitTimeout);
    Y_ENSURE(devices.size() == 1);

    return CreateSpdkEndpointClient(
        std::move(env),
        std::move(volumeClient),
        devices[0]);
}

IBlockStorePtr CreateSCSIEndpointClient(
    NSpdk::ISpdkEnvPtr env,
    IBlockStorePtr volumeClient,
    const TSCSIEndpointConfig& config)
{
    auto targetUrl = config.DeviceUrl + config.SocketPath + "/0";

    auto future = env->RegisterSCSIDevice(
        "scsi",
        targetUrl,
        config.InitiatorIqn ? config.InitiatorIqn : IQN_CLIENT);

    auto deviceName = future.GetValue(WaitTimeout);

    return CreateSpdkEndpointClient(
        std::move(env),
        std::move(volumeClient),
        deviceName);
}

}   // namespace NCloud::NBlockStore::NClient
