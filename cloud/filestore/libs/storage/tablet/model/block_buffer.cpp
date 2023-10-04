#include "block_buffer.h"

#include <cloud/filestore/libs/storage/core/utils.h>

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
        Y_VERIFY(block.size() == ByteRange.BlockSize);

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

    TStringBuf GetContentRef() override
    {
        return Buffer;
    }

    TString GetContent() override
    {
        return Buffer;
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
    Y_VERIFY(buffer.size() == byteRange.Length);
    return std::make_shared<TBlockBuffer>(byteRange, std::move(buffer));
}

}   // namespace NCloud::NFileStore::NStorage
