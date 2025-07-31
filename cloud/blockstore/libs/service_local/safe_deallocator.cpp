#include "safe_deallocator.h"

#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>

#include <cloud/storage/core/libs/common/file_io_service.h>

#include <util/random/fast.h>
#include <util/string/builder.h>

#include <memory>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFree
{
    void operator () (void* ptr) const
    {
        std::free(ptr);
    }
};

using TAlignedBuffer = std::unique_ptr<char, TFree>;

TAlignedBuffer Allocate(size_t byteCount, int value = 0)
{
    TAlignedBuffer ptr{
        static_cast<char*>(std::aligned_alloc(DefaultBlockSize, byteCount))};
    std::memset(ptr.get(), value, byteCount);

    return ptr;
}

////////////////////////////////////////////////////////////////////////////////

class TSafeDeallocator
    : public std::enable_shared_from_this<TSafeDeallocator>
{
private:
    static constexpr ui64 ValidatedBlocksRatio = 1000; // 0.1% of blocks in device
    static constexpr ui64 MaxDeallocateChunkSize = 1_GB;

    const TString Filename;
    TFileHandle Fd;

    const ui64 StartIndex;
    const ui64 BlocksCount;
    const ui32 BlockSize;

    const IFileIOServicePtr FileIO;
    const NNvme::INvmeManagerPtr NvmeManager;

    TPromise<NProto::TError> Response;

    ui64 ValidateRemainingBlockCount;
    ui64 ValidatedBlockIndex = 0;
    ui64 DeallocateNextBlockIndex = 0;
    TAlignedBuffer Buffer;
    TAlignedBuffer ZeroBuffer;
    TAlignedBuffer FFBuffer;
    TFastRng64 Rand;

public:
    TSafeDeallocator(
            TString filename,
            TFileHandle fd,
            IFileIOServicePtr fileIO,
            ui64 startIndex,
            ui64 blocksCount,
            ui32 blockSize,
            NNvme::INvmeManagerPtr nvmeManager)
        : Filename(std::move(filename))
        , Fd(std::move(fd))
        , StartIndex(startIndex)
        , BlocksCount(blocksCount)
        , BlockSize(blockSize)
        , FileIO(std::move(fileIO))
        , NvmeManager(std::move(nvmeManager))
        , Response(NewPromise<NProto::TError>())
        , ValidateRemainingBlockCount(BlocksCount / ValidatedBlocksRatio)
        , DeallocateNextBlockIndex(StartIndex)
        , Buffer(Allocate(BlockSize))
        , ZeroBuffer(Allocate(BlockSize))
        , FFBuffer(Allocate(BlockSize, 0xFF))
        , Rand(GetCycleCount())
    {}

    TFuture<NProto::TError> Deallocate()
    {
        DeallocateNextChunk();

        return Response.GetFuture();
    }

private:
    void DeallocateNextChunk()
    {
        const auto remainingBlockCount =
            StartIndex + BlocksCount - DeallocateNextBlockIndex;
        if (remainingBlockCount == 0) {
            ValidateNextBlock();
            return;
        }

        const auto deallocateBlockCount =
            std::min(remainingBlockCount, MaxDeallocateChunkSize / BlockSize);

        auto future = NvmeManager->Deallocate(
            Filename,
            DeallocateNextBlockIndex * BlockSize,
            deallocateBlockCount * BlockSize);

        DeallocateNextBlockIndex += deallocateBlockCount;

        future.Subscribe([self = shared_from_this()] (auto& future) {
            const auto& error = future.GetValue();
            if (HasError(error)) {
                self->Response.SetValue(error);
                return;
            }

            self->DeallocateNextChunk();
        });
    }

    void ValidateNextBlock()
    {
        if (ValidateRemainingBlockCount == 0) {
            Response.SetValue(NProto::TError());
            return;
        }

        ValidateRemainingBlockCount--;

        ValidatedBlockIndex = StartIndex + Rand.Uniform(BlocksCount);

        FileIO->AsyncRead(
            Fd,
            static_cast<i64>(ValidatedBlockIndex * BlockSize),
            {Buffer.get(), BlockSize},
            std::bind_front(
                &TSafeDeallocator::ReadCallback,
                shared_from_this()));
    }

    void ReadCallback(const NProto::TError& error, ui32 bytes)
    {
        if (HasError(error)) {
            Response.SetValue(error);
            return;
        }

        if (bytes != BlockSize) {
            Response.SetValue(
                MakeError(E_IO, TStringBuilder() <<
                    "dealloc validator read " << bytes << " bytes"));
            return;
        }

        if (!IsDeallocatedBlock()) {
            Response.SetValue(
                MakeError(E_IO, TStringBuilder() <<
                    "read non 00/ff block after deallocate at index " <<
                    ValidatedBlockIndex));
            return;
        }

        ValidateNextBlock();
    }

    bool IsDeallocatedBlock()
    {
        int res = memcmp(Buffer.get(), ZeroBuffer.get(), BlockSize);
        if (res == 0) {
            return true;
        }

        // some vendors can return FF in deallocated block
        res = memcmp(Buffer.get(), FFBuffer.get(), BlockSize);
        return res == 0;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TError> SafeDeallocateDevice(
    TString filename,
    TFileHandle fd,
    IFileIOServicePtr fileIO,
    ui64 startIndex,
    ui64 blocksCount,
    ui32 blockSize,
    NNvme::INvmeManagerPtr nvmeManager)
{
    Y_ENSURE(fileIO);
    Y_ENSURE(nvmeManager);

    auto deallocator = std::make_shared<TSafeDeallocator>(
        std::move(filename),
        std::move(fd),
        std::move(fileIO),
        startIndex,
        blocksCount,
        blockSize,
        std::move(nvmeManager));

    return deallocator->Deallocate();
}

}   // namespace NCloud::NBlockStore::NServer
