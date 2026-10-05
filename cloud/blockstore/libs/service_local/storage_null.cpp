#include "storage_null.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/latency.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>

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
        auto latency = StartLatency(callContext);
        auto future = [&]() -> TFuture<NProto::TReadBlocksLocalResponse>
        {
            Y_UNUSED(callContext);

            const auto blockSize = request->GetBlockSize();
            const auto blocksCount = request->GetBlocksCount();
            size_t responseSize = blockSize * blocksCount;

            auto guard = request->Sglist.Acquire();
            if (!guard) {
                NProto::TReadBlocksLocalResponse response = TErrorResponse(
                    E_CANCELLED,
                    "failed to acquire sglist in NullStorage");
                return MakeFuture(std::move(response));
            }

            // simulate zero response
            for (const auto& buf : guard.Get()) {
                if (responseSize == 0) {
                    break;
                }

                const auto size = std::min(buf.Size(), responseSize);
                if (buf.Data()) {
                    memset(const_cast<char*>(buf.Data()), 0, size);
                }
                responseSize -= size;
            }

            return MakeFuture(NProto::TReadBlocksLocalResponse());
        }();
        if (!latency) {
            return future;
        }
        return future.Apply(
            [latency](const auto& f)
            {
                auto response = f.GetValue();
                FinishLatencyLeaf(latency, response);
                return response;
            });
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        auto latency = StartLatency(callContext);
        auto future = [&]() -> TFuture<NProto::TWriteBlocksLocalResponse>
        {
                Y_UNUSED(callContext);
            Y_UNUSED(request);

            return MakeFuture(NProto::TWriteBlocksLocalResponse());
        }();
        if (!latency) {
            return future;
        }
        return future.Apply(
            [latency](const auto& f)
            {
                auto response = f.GetValue();
                FinishLatencyLeaf(latency, response);
                return response;
            });
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
    }
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

}   // namespace NCloud::NBlockStore::NServer
