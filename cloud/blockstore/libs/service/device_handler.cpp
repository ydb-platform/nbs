#include "device_handler.h"

#include "aligned_device_handler.h"
#include "unaligned_device_handler.h"

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MaxUnalignedRequestSize = 32_MB;

// Keep the value less than MaxBufferSize in
// cloud/blockstore/libs/rdma/iface/client.h
constexpr ui32 MaxSubRequestSize = 4_MB;


constexpr ui64 MaxZeroBlocksSubRequestSize = 2048_MB;

////////////////////////////////////////////////////////////////////////////////

struct TDefaultDeviceHandlerFactory final
    : public IDeviceHandlerFactory
{
    const ui32 MaxSubRequestSize;

    explicit TDefaultDeviceHandlerFactory(ui32 maxSubRequestSize)
        : MaxSubRequestSize(maxSubRequestSize)
    {}

    IDeviceHandlerPtr CreateDeviceHandler(
        IStoragePtr storage,
        TString diskId,
        TString clientId,
        ui32 blockSize,
        bool unalignedRequestsDisabled,
        bool checkBufferModificationDuringWriting,
        bool isReliableMediaKind,
        ui64 maxZeroBlocksSubRequestSize) override
    {
        auto maxZeroBlocksSubRequest = maxZeroBlocksSubRequestSize != 0
                                           ? static_cast<ui32>(std::min(
                                                 MaxZeroBlocksSubRequestSize,
                                                 maxZeroBlocksSubRequestSize))
                                           : MaxSubRequestSize;

        if (unalignedRequestsDisabled) {
            return std::make_shared<TAlignedDeviceHandler>(
                std::move(storage),
                std::move(diskId),
                std::move(clientId),
                blockSize,
                MaxSubRequestSize,
                maxZeroBlocksSubRequest,
                checkBufferModificationDuringWriting,
                isReliableMediaKind);
        }

        return std::make_shared<TUnalignedDeviceHandler>(
            std::move(storage),
            std::move(diskId),
            std::move(clientId),
            blockSize,
            MaxSubRequestSize,
            maxZeroBlocksSubRequest,
            MaxUnalignedRequestSize,
            checkBufferModificationDuringWriting,
            isReliableMediaKind);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDeviceHandlerFactoryPtr CreateDefaultDeviceHandlerFactory()
{
    return std::make_shared<TDefaultDeviceHandlerFactory>(MaxSubRequestSize);
}

IDeviceHandlerFactoryPtr CreateDeviceHandlerFactory(ui32 maxSubRequestSize)
{
    return std::make_shared<TDefaultDeviceHandlerFactory>(maxSubRequestSize);
}

}   // namespace NCloud::NBlockStore
