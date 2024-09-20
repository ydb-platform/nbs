#include "device_handler.h"

#include "aligned_device_handler.h"
#include "unaligned_device_handler.h"

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MaxUnalignedRequestSize = 32_MB;
constexpr ui32 MaxSubRequestSize = 4_MB;

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
        TString clientId,
        ui32 blockSize,
        bool unalignedRequestsDisabled) override
    {
        if (unalignedRequestsDisabled) {
            return std::make_shared<TAlignedDeviceHandler>(
                std::move(storage),
                std::move(clientId),
                blockSize,
                MaxSubRequestSize);
        }

        return std::make_shared<TUnalignedDeviceHandler>(
            std::move(storage),
            std::move(clientId),
            blockSize,
            MaxSubRequestSize,
            MaxUnalignedRequestSize);
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
