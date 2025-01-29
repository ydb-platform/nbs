#include "hash_table_storage.h"

#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct THashTableStorage final
    : public IStorage
{
    THashMap<ui64, TString> Blocks;
    ui32 BlockSize;
    ui64 BlockCount;

    THashTableStorage(ui32 blockSize, ui64 blockCount)
        : BlockSize(blockSize)
        , BlockCount(blockCount)
    {}

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(callContext);

        NProto::TZeroBlocksResponse response;
        auto b = request->GetStartIndex();
        auto e = request->GetStartIndex() + request->GetBlocksCount();

        if (e > BlockCount) {
            *response.MutableError() =
                MakeError(E_ARGUMENT, "index out of bounds");
        } else {
            while (b < e) {
                Blocks[b].clear();
                ++b;
            }
        }

        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);

        NProto::TReadBlocksLocalResponse response;

        auto guard = request->Sglist.Acquire();

        if (!guard) {
            *response.MutableError() = MakeError(
                E_CANCELLED,
                "failed to acquire sglist in HashTableStorage");
            return MakeFuture(std::move(response));
        }

        auto sglist = guard.Get();
        auto b = request->GetStartIndex();
        auto e = request->GetStartIndex() + request->GetBlocksCount();

        if (e > BlockCount) {
            response.MutableError()->CopyFrom(
                MakeError(E_ARGUMENT, "index out of bounds")
            );
            return MakeFuture(std::move(response));
        }

        while (b < e) {
            auto data = Blocks.FindPtr(b);
            auto& target = sglist[b - request->GetStartIndex()];
            if (!data || data->empty()) {
                memset(const_cast<char*>(target.Data()), 0, target.Size());
            } else {
                Y_ABORT_UNLESS(target.Size() == BlockSize);
                memcpy(const_cast<char*>(target.Data()), data->data(), BlockSize);
            }

            ++b;
        }

        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);

        NProto::TWriteBlocksLocalResponse response;

        auto guard = request->Sglist.Acquire();

        if (!guard) {
            *response.MutableError() = MakeError(
                E_CANCELLED,
                "failed to acquire sglist in HashTableStorage");
            return MakeFuture(std::move(response));
        }

        const auto& sglist = guard.Get();
        auto b = request->GetStartIndex();
        auto e = request->GetStartIndex() + request->BlocksCount;

        if (e > BlockCount) {
            response.MutableError()->CopyFrom(
                MakeError(E_ARGUMENT, "index out of bounds")
            );
            return MakeFuture(std::move(response));
        }

        TSgList dst(request->BlocksCount);

        while (b < e) {
            auto& block = Blocks[b];
            block.resize(request->BlockSize);
            dst[b - request->GetStartIndex()] = {block.data(), block.size()};
            ++b;
        }
        SgListCopy(sglist, dst);

        return MakeFuture(std::move(response));
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        switch (method) {
        case NProto::DEVICE_ERASE_METHOD_DEALLOCATE:
        case NProto::DEVICE_ERASE_METHOD_ZERO_FILL:
        case NProto::DEVICE_ERASE_METHOD_USER_DATA_ERASE:
            for (ui64 i = 0; i < BlockCount; i++) {
                Blocks[i].clear();
            }
            return MakeFuture(NProto::TError());

        case NProto::DEVICE_ERASE_METHOD_CRYPTO_ERASE:
            return MakeFuture(MakeError(E_FAIL, "unsupported erase method"));

        case NProto::DEVICE_ERASE_METHOD_NONE:
            return MakeFuture(NProto::TError());
        }
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

IStoragePtr CreateHashTableStorage(ui32 blockSize, ui64 blockCount)
{
    return std::make_shared<THashTableStorage>(blockSize, blockCount);
}

}   // namespace NCloud::NBlockStore::NStorage
