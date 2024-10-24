#include "block_buffer.h"

#include <cloud/filestore/libs/storage/model/utils.h>

#include <cloud/storage/core/libs/common/verify.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TBlockBuffer final
    : public IBlockBuffer
{
private:
    const TByteRange ByteRange;
    TString Buffer;

public:
    TBlockBuffer(TByteRange byteRange, TString buffer)
        : ByteRange(byteRange)
        , Buffer(std::move(buffer))
    {
    }

    TStringBuf GetUnalignedHead() override
    {
        const char* ptr = Buffer.data();
        return { ptr, ByteRange.UnalignedHeadLength() };
    }

    TStringBuf GetUnalignedTail() override
    {
        const auto offset = ByteRange.RelativeUnalignedTailOffset();
        const char* ptr = Buffer.data() + offset;
        return { ptr, ByteRange.UnalignedTailLength() };
    }

    TStringBuf GetBlock(size_t index) override
    {
        const auto offset = ByteRange.RelativeAlignedBlockOffset(index);
        const char* ptr = Buffer.data() + offset;
        return { ptr, ByteRange.BlockSize };
    }

    void SetBlock(size_t index, TStringBuf block) override
    {
        Y_ABORT_UNLESS(block.size() == ByteRange.BlockSize);

        const auto offset = ByteRange.RelativeAlignedBlockOffset(index);
        char* ptr = const_cast<char*>(Buffer.data()) + offset;
        memcpy(ptr, block.data(), ByteRange.BlockSize);
    }

    void ClearBlock(size_t index) override
    {
        const auto offset = ByteRange.RelativeAlignedBlockOffset(index);
        char* ptr = const_cast<char*>(Buffer.data()) + offset;
        memset(ptr, 0, ByteRange.BlockSize);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLazyBlockBuffer final
    : public IBlockBuffer
{
private:
    const TByteRange ByteRange;
    TString UnalignedHead;
    TVector<TString> Blocks;
    TString UnalignedTail;

public:
    explicit TLazyBlockBuffer(TByteRange byteRange)
        : ByteRange(byteRange)
        , Blocks(ByteRange.BlockCount())
    {
    }

    TStringBuf GetUnalignedHead() override
    {
        if (!UnalignedHead) {
            UnalignedHead.ReserveAndResize(ByteRange.UnalignedHeadLength());
        }

        return UnalignedHead;
    }

    TStringBuf GetUnalignedTail() override
    {
        if (!UnalignedTail) {
            UnalignedTail.ReserveAndResize(ByteRange.UnalignedTailLength());
        }

        return UnalignedTail;
    }

    TStringBuf GetBlock(size_t index) override
    {
        auto& block = Blocks[index];
        if (!block) {
            block.ReserveAndResize(ByteRange.BlockSize);
        }

        return block;
    }

    void SetBlock(size_t index, TStringBuf block) override
    {
        Y_ABORT_UNLESS(block.size() == ByteRange.BlockSize);
        Blocks[index] = block;
    }

    void ClearBlock(size_t index) override
    {
        Blocks[index].clear();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockBufferPtr CreateBlockBuffer(TByteRange byteRange)
{
    return std::make_shared<TBlockBuffer>(
        byteRange,
        TString(byteRange.Length, 0));
}

IBlockBufferPtr CreateBlockBuffer(TByteRange byteRange, TString buffer)
{
    Y_ABORT_UNLESS(buffer.size() == byteRange.Length);
    return std::make_shared<TBlockBuffer>(byteRange, std::move(buffer));
}

IBlockBufferPtr CreateLazyBlockBuffer(TByteRange byteRange)
{
    return std::make_shared<TLazyBlockBuffer>(byteRange);
}

////////////////////////////////////////////////////////////////////////////////

void CopyFileData(
    const TString& logTag,
    const TByteRange origin,
    const TByteRange aligned,
    const ui64 fileSize,
    IBlockBuffer& buffer,
    TString* out)
{
    const auto end = Min(fileSize, origin.End());
    if (end <= origin.Offset) {
        return;
    }

    out->ReserveAndResize(end - origin.Offset);
    char* outPtr = out->begin();

    ui32 i = 0;

    // processing unaligned head
    if (origin.Offset > aligned.Offset) {
        const auto block = buffer.GetBlock(i);
        const auto shift = origin.Offset - aligned.Offset;
        const auto sz = Min(block.size() - shift, out->size());
        STORAGE_VERIFY(
            outPtr + sz <= out->end(),
            TWellKnownEntityTypes::FILESYSTEM,
            logTag);
        memcpy(outPtr, block.data() + shift, sz);
        outPtr += sz;

        ++i;
    }

    while (i < aligned.BlockCount()) {
        const auto block = buffer.GetBlock(i);
        // Min needed to properly process unaligned tail
        const auto len = Min<ui64>(out->end() - outPtr, block.size());
        if (!len) {
            // origin.End() is greater than fileSize
            break;
        }
        memcpy(outPtr, block.data(), len);
        outPtr += len;

        ++i;
    }
}

}   // namespace NCloud::NFileStore::NStorage
