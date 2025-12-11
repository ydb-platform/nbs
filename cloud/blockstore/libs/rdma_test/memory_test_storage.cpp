#include "memory_test_storage.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

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

void TMemoryTestStorage::SetHandbrake(TFuture<void> handbrake)
{
    Handbrake = std::move(handbrake);
}

auto TMemoryTestStorage::LockRange(TBlockRange32 range)
    -> TList<TMemoryTestStorage::TCurrentOperationPtr>::iterator
{
    // Wait handbrake.
    TFuture<void> handbrake;
    {
        TGuard<TAdaptiveLock> guard(Lock);
        handbrake = std::move(Handbrake);
    }
    if (handbrake.Initialized()) {
        handbrake.GetValueSync();
    }

    // Lock range.
    TList<TCurrentOperationPtr>::iterator result;
    for (;;) {
        TCurrentOperationPtr operationToWait;
        {
            TGuard<TAdaptiveLock> guard(Lock);
            for (auto& operation: CurrentOperations) {
                if (range.Overlaps(operation->Range)) {
                    operationToWait = operation;
                    break;
                }
            }
            if (!operationToWait) {
                result = CurrentOperations.emplace(
                    CurrentOperations.end(),
                    std::make_shared<TCurrentOperation>(range));
                break;
            }
        }
        if (operationToWait) {
            operationToWait->DoneEvent.Wait();
        }
    }

    return result;
}

void TMemoryTestStorage::UnlockRange(
    TList<TCurrentOperationPtr>::iterator lockIt)
{
    TGuard<TAdaptiveLock> guard(Lock);
    (*lockIt)->DoneEvent.Signal();
    CurrentOperations.erase(lockIt);
}

char* TMemoryTestStorage::GetPtr(ui32 blockIndex, ui32 blockSize)
{
    UNIT_ASSERT(blockIndex < Data.size() / blockSize);
    return Data.data() + blockSize * blockIndex;
}

TFuture<NProto::TWriteBlocksLocalResponse>
TMemoryTestStorage::DoWriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    Y_UNUSED(callContext);

    auto guard = request->Sglist.Acquire();
    UNIT_ASSERT(guard);
    const TSgList& sgList = guard.Get();
    auto range = TBlockRange32::WithLength(
        request->GetStartIndex(),
        SgListGetSize(sgList) / request->BlockSize);

    auto lockIt = LockRange(range);

    char* ptr = GetPtr(request->GetStartIndex(), request->BlockSize);
    for (auto dataRef: sgList) {
        std::memcpy(ptr, dataRef.Data(), dataRef.Size());
        ptr += dataRef.Size();
    }

    UnlockRange(lockIt);

    NProto::TWriteBlocksLocalResponse response;
    return MakeFuture(response);
}

TFuture<NProto::TReadBlocksLocalResponse> TMemoryTestStorage::DoReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
{
    Y_UNUSED(callContext);

    auto guard = request->Sglist.Acquire();
    UNIT_ASSERT(guard);
    const TSgList& sgList = guard.Get();
    UNIT_ASSERT_EQUAL(request->GetBlocksCount(), sgList.size());

    auto range = TBlockRange32::WithLength(
        request->GetStartIndex(),
        request->GetBlocksCount());

    auto lockIt = LockRange(range);

    ui32 totalBlocksToRead = request->GetBlocksCount();
    ui32 totalBlocksRead = 0;
    for (auto dataRef: sgList) {
        UNIT_ASSERT(dataRef.Size() % request->BlockSize == 0);

        ui32 blocksToRead = std::min(
            totalBlocksToRead,
            static_cast<ui32>(dataRef.Size() / request->BlockSize));

        char* ptr = GetPtr(
            request->GetStartIndex() + totalBlocksRead,
            request->BlockSize);
        std::memcpy(
            const_cast<char*>(dataRef.Data()),
            ptr,
            blocksToRead * request->BlockSize);
        totalBlocksToRead -= blocksToRead;
        totalBlocksRead += blocksToRead;
    }
    UNIT_ASSERT(totalBlocksToRead == 0);
    UNIT_ASSERT(totalBlocksRead == request->GetBlocksCount());

    UnlockRange(lockIt);

    NProto::TReadBlocksLocalResponse response;
    return MakeFuture(response);
}

TFuture<NProto::TZeroBlocksResponse> TMemoryTestStorage::DoZeroBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    Y_UNUSED(callContext);

    auto range = TBlockRange32::WithLength(
        request->GetStartIndex(),
        request->GetBlocksCount());

    auto lockIt = LockRange(range);

    ui32 BlockSize = 4_KB;

    char* ptr = GetPtr(request->GetStartIndex(), BlockSize);
    std::memset(ptr, 0, BlockSize * request->GetBlocksCount());

    UnlockRange(lockIt);

    NProto::TZeroBlocksResponse response;
    return MakeFuture(response);
}

}   // namespace NCloud::NBlockStore::NStorage
