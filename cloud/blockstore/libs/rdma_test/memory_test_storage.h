#pragma once

#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/storage_test.h>

#include <library/cpp/threading/future/core/future.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TMemoryTestStorage: public TTestStorage
{
private:
    TVector<char> Data;
    NThreading::TFuture<void> Handbrake;

public:
    TMemoryTestStorage(size_t deviceSize);

    void SetHandbrake(NThreading::TFuture<void> handbrake);

private:
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
