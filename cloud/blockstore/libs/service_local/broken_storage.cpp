#include <cloud/blockstore/libs/service/storage.h>

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

class TBrokenStorage final: public IStorage
{
private:
    template <typename T>
    auto MakeResponse() const
    {
        return MakeErrorFuture<T>(
            std::make_exception_ptr(TServiceError(E_IO) << "device is broken"));
    }

public:
    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return MakeResponse<NProto::TZeroBlocksResponse>();
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return MakeResponse<NProto::TReadBlocksLocalResponse>();
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return MakeResponse<NProto::TWriteBlocksLocalResponse>();
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);

        return MakeResponse<NProto::TError>();
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

IStoragePtr CreateBrokenStorage()
{
    return std::make_shared<TBrokenStorage>();
}

}   // namespace NCloud::NBlockStore::NStorage
