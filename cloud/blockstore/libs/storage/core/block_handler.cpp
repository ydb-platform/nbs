#include "block_handler.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/common/request_checksum_helpers.h>

#include <util/generic/bitmap.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

void SetBitMapValue(TDynBitMap& bitmap, size_t index, bool value)
{
    if (value) {
        bitmap.Set(index);
    } else {
        bitmap.Reset(index);
    }
}

TStringBuf ConvertBitMapToStringBuf(const TDynBitMap& bitmap)
{
    auto size = bitmap.Size() / 8;
    Y_ABORT_UNLESS(bitmap.GetChunkCount() * sizeof(TDynBitMap::TChunk) == size);
    return { reinterpret_cast<const char*>(bitmap.GetChunks()), size };
}

////////////////////////////////////////////////////////////////////////////////

class TWriteBlocksHandler final
    : public IWriteBlocksHandler
{
private:
    const TBlockRange64 WriteRange;
    const TGuardedBuffer<NProto::TWriteBlocksRequest> Request;
    const ui32 BlockSize;

    TSgList SgList;

public:
    TWriteBlocksHandler(
            const TBlockRange64& writeRange,
            std::unique_ptr<TEvService::TEvWriteBlocksRequest> request,
            ui32 blockSize)
        : WriteRange(writeRange)
        , Request(std::move(request->Record))
        , BlockSize(blockSize)
    {
        auto sglistOrError = SgListNormalize(
            GetSgList(Request.Get()),
            BlockSize);
        Y_ABORT_UNLESS(!HasError(sglistOrError));

        SgList = sglistOrError.ExtractResult();
        Y_ABORT_UNLESS(SgList.size() == writeRange.Size());
    }

    TGuardedSgList GetBlocks(const TBlockRange64& range) override
    {
        Y_ABORT_UNLESS(WriteRange.Contains(range));
        TSgList blocks(Reserve(range.Size()));

        for (ui64 blockIndex: xrange(range)) {
            Y_ABORT_UNLESS(WriteRange.Contains(blockIndex));
            size_t index = blockIndex - WriteRange.Start;
            blocks.emplace_back(SgList[index]);
        }

        return Request.CreateGuardedSgList(std::move(blocks));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReadBlocksHandler final
    : public IReadBlocksHandler
{
private:
    const TBlockRange64 ReadRange;
    const ui32 BlockSize;
    const bool EnableChecksumValidation;

    NProto::TIOVector Blocks;
    TGuardedSgList SgList;
    TVector<bool> BlockMarks;
    TDynBitMap UnencryptedBlockMask;

public:
    TReadBlocksHandler(
        const TBlockRange64& readRange,
        ui32 blockSize,
        bool enableChecksumValidation)
        : ReadRange(readRange)
        , BlockSize(blockSize)
        , EnableChecksumValidation(enableChecksumValidation)
        , BlockMarks(ReadRange.Size(), false)
    {
        UnencryptedBlockMask.Reserve(ReadRange.Size());
        Blocks = PrepareBuffers(readRange.Size());
    }

    ~TReadBlocksHandler()
    {
        SgList.Close();
    }

    TGuardedSgList GetGuardedSgList(
        const TVector<ui64>& blockIndices,
        bool baseDisk) override
    {
        TSgList sglist(Reserve(blockIndices.size()));

        for (const auto blockIndex: blockIndices) {
            Y_ABORT_UNLESS(ReadRange.Contains(blockIndex));

            size_t index = blockIndex - ReadRange.Start;
            auto& block = *Blocks.MutableBuffers(index);
            block.ReserveAndResize(BlockSize);
            char* head = block.begin();
            sglist.emplace_back(head, BlockSize);

            BlockMarks[index] = true;
            SetBitMapValue(UnencryptedBlockMask, index, baseDisk);
        }

        return SgList.Create(std::move(sglist));
    }

    bool SetBlock(
        ui64 blockIndex,
        TBlockDataRef blockContent,
        bool baseDisk) override
    {
        Y_ABORT_UNLESS(ReadRange.Contains(blockIndex));
        size_t index = blockIndex - ReadRange.Start;
        auto& block = *Blocks.MutableBuffers(index);

        if (blockContent.Data() != nullptr) {
            Y_ABORT_UNLESS(blockContent.Size() == BlockSize);
            block.assign(blockContent.AsStringBuf());
            SetBitMapValue(UnencryptedBlockMask, index, baseDisk);
        } else {
            block.clear();  // do not keep zero blocks
            SetBitMapValue(UnencryptedBlockMask, index, true);
        }

        BlockMarks[index] = true;
        return true;
    }

    void Clear() override
    {
        std::fill(BlockMarks.begin(), BlockMarks.end(), false);
        UnencryptedBlockMask.Clear();
    }

    void GetResponse(NProto::TReadBlocksResponse& response) override
    {
        bool allZeroes = true;
        for (size_t i = 0; i < BlockMarks.size(); ++i) {
            auto& block = *Blocks.MutableBuffers(i);
            if (!BlockMarks[i]) {
                block.clear();
                SetBitMapValue(UnencryptedBlockMask, i, true);
            } else {
                allZeroes =
                    allZeroes && IsAllZeroes(block.data(), block.size());
            }
        }

        if (EnableChecksumValidation) {
            *response.MutableChecksum() = CalculateChecksum(Blocks, BlockSize);
        }

        // Wait for all TSgList users are done.
        SgList.Close();
        response.MutableBlocks()->Swap(&Blocks);

        auto stringBuf = ConvertBitMapToStringBuf(UnencryptedBlockMask);
        auto& blockMask = *response.MutableUnencryptedBlockMask();
        blockMask.assign(stringBuf);

        response.SetAllZeroes(allZeroes);
    }

    TGuardedSgList GetLocalResponse(
        NProto::TReadBlocksLocalResponse& response) override
    {
        Y_UNUSED(response);
        Y_ABORT("Not supported");
    }

private:
    static NProto::TIOVector PrepareBuffers(ui32 blocksCount)
    {
        NProto::TIOVector iov;

        auto& buffers = *iov.MutableBuffers();
        buffers.Reserve(blocksCount);

        for (size_t i = 0; i < blocksCount; ++i) {
            buffers.Add();  // empty by default
        }

        return iov;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBlocksLocalHandler final
    : public IWriteBlocksHandler
{
private:
    const TBlockRange64 WriteRange;
    TGuardedSgList GuardedSgList;

public:
    TWriteBlocksLocalHandler(
            const TBlockRange64& writeRange,
            std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest> request)
        : WriteRange(writeRange)
        , GuardedSgList(std::move(request->Record.Sglist))
    {
        const auto blocksCount = request->Record.BlocksCount;
        Y_ABORT_UNLESS(WriteRange.Size() == blocksCount);
    }

    TGuardedSgList GetBlocks(const TBlockRange64& range) override
    {
        Y_ABORT_UNLESS(WriteRange.Contains(range));

        if (auto guard = GuardedSgList.Acquire()) {
            const auto& sgList = guard.Get();
            TSgList blocks(Reserve(range.Size()));

            for (ui64 blockIndex: xrange(range)) {
                Y_ABORT_UNLESS(WriteRange.Contains(blockIndex));
                size_t index = blockIndex - WriteRange.Start;
                blocks.push_back(sgList[index]);
            }

            return GuardedSgList.Create(std::move(blocks));
        }

        return GuardedSgList;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReadBlocksLocalHandler final
    : public IReadBlocksHandler
{
private:
    const TBlockRange64 ReadRange;
    const ui32 BlockSize;
    const bool EnableChecksumValidation;

    TGuardedSgList GuardedSgList;
    TVector<bool> BlockMarks;
    TDynBitMap UnencryptedBlockMask;

public:
    TReadBlocksLocalHandler(
            const TBlockRange64& readRange,
            TGuardedSgList guardedSgList,
            ui32 blockSize,
            bool enableChecksumValidation)
        : ReadRange(readRange)
        , BlockSize(blockSize)
        , EnableChecksumValidation(enableChecksumValidation)
        , GuardedSgList(std::move(guardedSgList))
        , BlockMarks(ReadRange.Size(), false)
    {
        UnencryptedBlockMask.Reserve(ReadRange.Size());
        UnencryptedBlockMask.Clear();
    }

    TGuardedSgList GetGuardedSgList(
        const TVector<ui64>& blockIndices,
        bool baseDisk) override
    {
        if (auto guard = GuardedSgList.Acquire()) {
            const auto& src = guard.Get();
            TSgList subset(Reserve(blockIndices.size()));

            for (auto blockIndex: blockIndices) {
                Y_ABORT_UNLESS(ReadRange.Contains(blockIndex));
                const auto index = blockIndex - ReadRange.Start;
                subset.push_back(src[index]);

                BlockMarks[index] = true;
                SetBitMapValue(UnencryptedBlockMask, index, baseDisk);
            }

            return GuardedSgList.Create(std::move(subset));
        }

        return GuardedSgList;
    }

    bool SetBlock(
        ui64 blockIndex,
        TBlockDataRef blockContent,
        bool baseDisk) override
    {
        Y_ABORT_UNLESS(ReadRange.Contains(blockIndex));
        size_t index = blockIndex - ReadRange.Start;

        if (auto guard = GuardedSgList.Acquire()) {
            const auto& sglist = guard.Get();

            Y_ABORT_UNLESS(index < sglist.size());
            TBlockDataRef block = sglist[index];
            Y_ABORT_UNLESS(block.Size() == BlockSize);

            if (blockContent.Data() != nullptr) {
                Y_ABORT_UNLESS(blockContent.Size() == BlockSize);
                auto* data = const_cast<char*>(block.Data());
                memcpy(data, blockContent.Data(), BlockSize);
                SetBitMapValue(UnencryptedBlockMask, index, baseDisk);
            } else {
                auto* data = const_cast<char*>(block.Data());
                memset(data, 0, BlockSize);  // zero missing blocks
                SetBitMapValue(UnencryptedBlockMask, index, true);
            }

            BlockMarks[index] = true;
            return true;
        }

        return false;
    }

    void Clear() override
    {
        std::fill(BlockMarks.begin(), BlockMarks.end(), false);
        UnencryptedBlockMask.Clear();
    }

    void GetResponse(NProto::TReadBlocksResponse& response) override
    {
        Y_UNUSED(response);
        Y_ABORT("Not supported");
    }

    TGuardedSgList GetLocalResponse(
        NProto::TReadBlocksLocalResponse& response) override
    {
        if (auto guard = GuardedSgList.Acquire()) {
            const auto& sglist = guard.Get();
            Y_ABORT_UNLESS(sglist.size() == BlockMarks.size());

            bool allZeroes = true;
            for (size_t i = 0; i < BlockMarks.size(); ++i) {
                if (!BlockMarks[i]) {
                    auto* data = const_cast<char*>(sglist[i].Data());
                    memset(data, 0, BlockSize);
                    SetBitMapValue(UnencryptedBlockMask, i, true);
                } else {
                    allZeroes = allZeroes && IsAllZeroes(sglist[i]);
                }
            }

            response.SetAllZeroes(allZeroes);

            if (EnableChecksumValidation) {
                *response.MutableChecksum() = CalculateChecksum(sglist);
            }
        }

        auto stringBuf = ConvertBitMapToStringBuf(UnencryptedBlockMask);
        auto& blockMask = *response.MutableUnencryptedBlockMask();
        blockMask.assign(stringBuf);

        return GuardedSgList;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMixedWriteBlocksHandler final
    : public IWriteBlocksHandler
{
    using TWriteHandlerAndRange =
        std::pair<IWriteBlocksHandlerPtr, TBlockRange64>;

private:
    TVector<TWriteHandlerAndRange> Parts;

public:
    TMixedWriteBlocksHandler(TVector<TWriteHandlerAndRange> parts)
        : Parts(std::move(parts))
    {}

    TGuardedSgList GetBlocks(const TBlockRange64& range) override
    {
        TVector<TGuardedSgList> sgLists;
        ui64 blockIndex = range.End + 1;

        while (blockIndex > range.Start) {
            auto endIndex = blockIndex - 1;
            auto it = UpperBound(
                Parts.begin(),
                Parts.end(),
                endIndex,
                [] (ui64 blockIndex, const TWriteHandlerAndRange& x) {
                    return blockIndex < x.second.Start;
                }
            );

            Y_ABORT_UNLESS(it != Parts.begin());
            --it;
            const auto& itHandler = it->first;
            const auto& itRange = it->second;

            Y_ABORT_UNLESS(itRange.End >= endIndex);

            blockIndex = Max(itRange.Start, range.Start);
            auto sgList = itHandler->GetBlocks(
                TBlockRange64::MakeClosedInterval(blockIndex, endIndex));
            sgLists.push_back(std::move(sgList));
        }

        std::reverse(sgLists.begin(), sgLists.end());
        return TGuardedSgList::CreateUnion(std::move(sgLists));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IWriteBlocksHandlerPtr CreateWriteBlocksHandler(
    const TBlockRange64& writeRange,
    std::unique_ptr<TEvService::TEvWriteBlocksRequest> request,
    ui32 blockSize)
{
    return std::make_shared<TWriteBlocksHandler>(
        writeRange,
        std::move(request),
        blockSize);
}

IWriteBlocksHandlerPtr CreateWriteBlocksHandler(
    const TBlockRange64& writeRange,
    std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest> request)
{
    return std::make_shared<TWriteBlocksLocalHandler>(
        writeRange,
        std::move(request));
}

IReadBlocksHandlerPtr CreateReadBlocksHandler(
    const TBlockRange64& readRange,
    ui32 blockSize,
    bool enableChecksumValidation)
{
    return std::make_shared<TReadBlocksHandler>(
        readRange,
        blockSize,
        enableChecksumValidation);
}

IReadBlocksHandlerPtr CreateReadBlocksHandler(
    const TBlockRange64& readRange,
    const TGuardedSgList& sglist,
    ui32 blockSize,
    bool enableChecksumValidation)
{
    return std::make_shared<TReadBlocksLocalHandler>(
        readRange,
        sglist,
        blockSize,
        enableChecksumValidation);
}

IWriteBlocksHandlerPtr CreateMixedWriteBlocksHandler(
    TVector<std::pair<IWriteBlocksHandlerPtr, TBlockRange64>> parts)
{
    return std::make_shared<TMixedWriteBlocksHandler>(std::move(parts));
}

}   // namespace NCloud::NBlockStore::NStorage
