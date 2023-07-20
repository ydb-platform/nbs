#include "memory_test_storage.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TMemoryTestStorage::TMemoryTestStorage(size_t deviceSize)
    : Data(deviceSize, 0)
{
    WriteBlocksLocalHandler =
        [this](
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
    {
        return DoWriteBlocksLocal(std::move(callContext), std::move(request));
    };

    ReadBlocksLocalHandler =
        [this](
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
    {
        return DoReadBlocksLocal(std::move(callContext), std::move(request));
    };

    ZeroBlocksHandler = [this](
                            TCallContextPtr callContext,
                            std::shared_ptr<NProto::TZeroBlocksRequest> request)
    {
        return DoZeroBlocks(std::move(callContext), std::move(request));
    };
}

void TMemoryTestStorage::SetHandbrake(NThreading::TFuture<void> handbrake)
{
    Handbrake = std::move(handbrake);
}

char* TMemoryTestStorage::GetPtr(ui32 blockIndex, ui32 blockSize)
{
    UNIT_ASSERT(blockIndex < Data.size() / blockSize);
    return Data.data() + blockSize * blockIndex;
}

NThreading::TFuture<NProto::TWriteBlocksLocalResponse> TMemoryTestStorage::
    DoWriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    Y_UNUSED(callContext);

    auto handbrake = std::move(Handbrake);
    if (handbrake.Initialized()) {
        handbrake.GetValueSync();
    }

    auto guard = request->Sglist.Acquire();
    UNIT_ASSERT(guard);
    const TSgList& sgList = guard.Get();

    char* ptr = GetPtr(request->GetStartIndex(), request->BlockSize);
    for (ui32 i = 0; i < sgList.size(); ++i) {
        std::memcpy(ptr, sgList[i].Data(), sgList[i].Size());
        ptr += sgList[i].Size();
    }

    NProto::TWriteBlocksLocalResponse response;
    return NThreading::MakeFuture(response);
}

NThreading::TFuture<NProto::TReadBlocksLocalResponse> TMemoryTestStorage::
    DoReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
{
    Y_UNUSED(callContext);

    auto handbrake = std::move(Handbrake);
    if (handbrake.Initialized()) {
        handbrake.GetValueSync();
    }

    auto guard = request->Sglist.Acquire();
    UNIT_ASSERT(guard);
    const TSgList& sgList = guard.Get();
    UNIT_ASSERT_EQUAL(request->GetBlocksCount(), sgList.size());

    ui32 totalBlocksToRead = request->GetBlocksCount();
    ui32 totalBlocksRead = 0;
    for (ui32 i = 0; i < sgList.size(); ++i) {
        UNIT_ASSERT(sgList[i].Size() % request->BlockSize == 0);

        ui32 blocksToRead = std::min(
            totalBlocksToRead,
            static_cast<ui32>(sgList[i].Size() / request->BlockSize));

        char* ptr = GetPtr(
            request->GetStartIndex() + totalBlocksRead,
            request->BlockSize);
        std::memcpy(
            const_cast<char*>(sgList[i].Data()),
            ptr,
            blocksToRead * request->BlockSize);
        totalBlocksToRead -= blocksToRead;
        totalBlocksRead += blocksToRead;
    }
    UNIT_ASSERT(totalBlocksToRead == 0);
    UNIT_ASSERT(totalBlocksRead == request->GetBlocksCount());

    NProto::TReadBlocksLocalResponse response;
    return NThreading::MakeFuture(response);
}

NThreading::TFuture<NProto::TZeroBlocksResponse> TMemoryTestStorage::
    DoZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    Y_UNUSED(callContext);

    auto handbrake = std::move(Handbrake);
    if (handbrake.Initialized()) {
        handbrake.GetValueSync();
    }

    ui32 BlockSize = 4_KB;

    char* ptr = GetPtr(request->GetStartIndex(), BlockSize);
    std::memset(ptr, 0, BlockSize * request->GetBlocksCount());

    NProto::TZeroBlocksResponse response;
    return NThreading::MakeFuture(response);
}

}   // namespace NCloud::NBlockStore::NStorage
