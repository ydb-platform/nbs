#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/storage_test.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/list.h>
#include <util/system/event.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TMemoryTestStorage: public TTestStorage
{
private:
    // The storage must provide a guarantee of the atomicity of writing and
    // reading. This requires tracking inflight requests and executing them
    // sequentially if they overlap.
    struct TCurrentOperation
    {
        TBlockRange32 Range;
        TManualEvent DoneEvent;

        TCurrentOperation(TBlockRange32 range)
            : Range(range)
        {}
    };
    using TCurrentOperationPtr = std::shared_ptr<TCurrentOperation>;

    TVector<char> Data;
    NThreading::TFuture<void> Handbrake;
    TList<TCurrentOperationPtr> CurrentOperations;
    TAdaptiveLock Lock;

public:
    TMemoryTestStorage(size_t deviceSize);

    void SetHandbrake(NThreading::TFuture<void> handbrake);

private:
    TList<TCurrentOperationPtr>::iterator LockRange(TBlockRange32 range);
    void UnlockRange(TList<TCurrentOperationPtr>::iterator lockIt);

    char* GetPtr(ui32 blockIndex, ui32 blockSize);

    NThreading::TFuture<NProto::TWriteBlocksLocalResponse> DoWriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request);

    NThreading::TFuture<NProto::TReadBlocksLocalResponse> DoReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request);

    NThreading::TFuture<NProto::TZeroBlocksResponse> DoZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
