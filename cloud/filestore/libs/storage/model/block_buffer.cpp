#include "block_buffer.h"

#include <cloud/filestore/libs/storage/model/utils.h>

#include <cloud/storage/core/libs/common/verify.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TBufferImpl>
class TBlockBuffer final: public IBlockBuffer
{
    static_assert(
        std::is_same_v<TBufferImpl, TString> ||
            std::is_same_v<TBufferImpl, TRcBuf>,
        "TBlockBuffer supports only TString and TRcBuf");

private:
    const TByteRange ByteRange;
    TBufferImpl Buffer;

    char* AccessMutableData()
    {
        if constexpr (std::is_same_v<TBufferImpl, TRcBuf>) {
            // Copy TString-backed buffer into an internal backend before
            // modification to avoid changing storage shared with other owners.
            // In std::string builds, TString::IsDetached() always returns true,
            // so TRcBuf::Detach() does not detect shared storage and therefore
            // does not copy it.
            //
            // This is a defensive measure: current payloads use
            // TRcBufInternalBackend, and the current build uses a copy-on-write
            // TString.
            if (Buffer.template ContainsNativeType<TString>()) {
                // TRcBuf::Copy replaces the TString backend with
                // TInternalBackend, so subsequent calls skip the copy.
                Buffer = TRcBuf::Copy(Buffer.GetContiguousSpan());
            }
        }

        return Buffer.Detach();
    }

public:
    TBlockBuffer(TByteRange byteRange, TBufferImpl buffer)
        : ByteRange(byteRange)
        , Buffer(std::move(buffer))
    {}

    TStringBuf GetUnalignedHead() const override
    {
        const char* ptr = Buffer.data();
        return {ptr, ByteRange.UnalignedHeadLength()};
    }

    TStringBuf GetUnalignedTail() const override
    {
        const auto offset = ByteRange.RelativeUnalignedTailOffset();
        const char* ptr = Buffer.data() + offset;
        return {ptr, ByteRange.UnalignedTailLength()};
    }

    TStringBuf GetBlock(size_t index) const override
    {
        const auto offset = ByteRange.RelativeAlignedBlockOffset(index);
        const char* ptr = Buffer.data() + offset;
        return {ptr, ByteRange.BlockSize};
    }

    void SetBlock(size_t index, TStringBuf block) override
    {
        Y_ABORT_UNLESS(block.size() == ByteRange.BlockSize);

        const auto offset = ByteRange.RelativeAlignedBlockOffset(index);
        char* ptr = AccessMutableData() + offset;
        memcpy(ptr, block.data(), ByteRange.BlockSize);
    }

    void ClearBlock(size_t index) override
    {
        const auto offset = ByteRange.RelativeAlignedBlockOffset(index);
        char* ptr = AccessMutableData() + offset;
        memset(ptr, 0, ByteRange.BlockSize);
    }

    bool UsesLazyAllocation() const override
    {
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLazyBlockBuffer final
    : public IBlockBuffer
{
private:
    const TByteRange ByteRange;
    mutable TString UnalignedHead;
    mutable TVector<TString> Blocks;
    mutable TString UnalignedTail;

public:
    explicit TLazyBlockBuffer(TByteRange byteRange)
        : ByteRange(byteRange)
        , Blocks(ByteRange.BlockCount())
    {
    }

    // This method is not thread-safe because it allocates data on its first
    // invocation.
    TStringBuf GetUnalignedHead() const override
    {
        if (!UnalignedHead) {
            UnalignedHead.ReserveAndResize(ByteRange.UnalignedHeadLength());
        }

        return UnalignedHead;
    }

    // This method is not thread-safe because it allocates data on its first
    // invocation.
    TStringBuf GetUnalignedTail() const override
    {
        if (!UnalignedTail) {
            UnalignedTail.ReserveAndResize(ByteRange.UnalignedTailLength());
        }

        return UnalignedTail;
    }

    // This method is not thread-safe because it allocates data on its first
    // invocation.
    TStringBuf GetBlock(size_t index) const override
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

    bool UsesLazyAllocation() const override
    {
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockBufferPtr CreateBlockBuffer(TByteRange byteRange)
{
    return std::make_shared<TBlockBuffer<TString>>(
        byteRange,
        TString(byteRange.Length, 0));
}

IBlockBufferPtr CreateBlockBuffer(TByteRange byteRange, TString buffer)
{
    Y_ABORT_UNLESS(buffer.size() == byteRange.Length);
    return std::make_shared<TBlockBuffer<TString>>(byteRange, std::move(buffer));
}

IBlockBufferPtr CreateBlockBuffer(TByteRange byteRange, TRcBuf buffer)
{
    Y_ABORT_UNLESS(buffer.size() == byteRange.Length);
    return std::make_shared<TBlockBuffer<TRcBuf>>(byteRange, std::move(buffer));
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
