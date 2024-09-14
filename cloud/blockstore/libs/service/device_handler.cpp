#include "device_handler.h"

#include "aligned_device_handler.h"
#include "unaligned_device_handler.h"

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDefaultDeviceHandlerFactory final
    : public IDeviceHandlerFactory
{
    IDeviceHandlerPtr CreateDeviceHandler(
        IStoragePtr storage,
        TString clientId,
        ui32 blockSize,
        ui32 maxBlockCount,
        bool unalignedRequestsDisabled) override
    {
        if (unalignedRequestsDisabled) {
            return std::make_shared<TAlignedDeviceHandler>(
                std::move(storage),
                std::move(clientId),
                blockSize,
                maxBlockCount);
        }

        return std::make_shared<TUnalignedDeviceHandler>(
            std::move(storage),
            std::move(clientId),
            blockSize,
            maxBlockCount);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDeviceHandlerFactoryPtr CreateDefaultDeviceHandlerFactory()
{
    return std::make_shared<TDefaultDeviceHandlerFactory>();
}

}   // namespace NCloud::NBlockStore
