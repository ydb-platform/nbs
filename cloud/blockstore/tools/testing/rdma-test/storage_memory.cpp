#include "storage.h"

#include "probes.h"

namespace NCloud::NBlockStore {

using namespace NThreading;

LWTRACE_USING(BLOCKSTORE_TEST_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMemoryStorage final: public IStorage
{
private:
    const TString Buffer;
    const size_t BlockSize;

public:
    TMemoryStorage(ui32 blockSize, ui32 blocksCount)
        : Buffer(static_cast<ui64>(blocksCount) * blockSize, 0)
        , BlockSize(blockSize)
    {}

    void Start() override
    {}
    void Stop() override
    {}

    TFuture<NProto::TError> ReadBlocks(
        TCallContextPtr callContext,
        TReadBlocksRequestPtr request,
        TGuardedSgList sglist) override
    {
        Y_UNUSED(callContext);

        ui64 offset = static_cast<ui64>(request->GetBlockIndex()) * BlockSize;
        ui64 length = static_cast<ui64>(request->GetBlocksCount()) * BlockSize;

        auto guard = sglist.Acquire();
        Y_ABORT_UNLESS(guard);

        auto buffer = TStringBuf(Buffer).SubString(offset, length);
        Y_ENSURE(buffer.length() == length);

        auto bytesCopied = CopyMemory(guard.Get(), buffer);
        Y_ENSURE(bytesCopied == length);

        return MakeFuture(NProto::TError());
    }

    TFuture<NProto::TError> WriteBlocks(
        TCallContextPtr callContext,
        TWriteBlocksRequestPtr request,
        TGuardedSgList sglist) override
    {
        Y_UNUSED(callContext);

        ui64 offset = static_cast<ui64>(request->GetBlockIndex()) * BlockSize;
        ui64 length = static_cast<ui64>(request->GetBlocksCount()) * BlockSize;

        auto guard = sglist.Acquire();
        Y_ABORT_UNLESS(guard);

        auto buffer = TStringBuf(Buffer).SubString(offset, length);
        Y_ENSURE(buffer.length() == length);

        auto bytesCopied = CopyMemory(buffer, guard.Get());
        Y_ENSURE(bytesCopied == length);

        return MakeFuture(NProto::TError());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateMemoryStorage(ui32 blockSize, ui32 blocksCount)
{
    return std::make_shared<TMemoryStorage>(blockSize, blocksCount);
}

}   // namespace NCloud::NBlockStore
