#include "iovector.h"

#include <util/string/builder.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TSgList ResizeIOVector(NProto::TIOVector& iov, ui32 blocksCount, ui32 blockSize)
{
    auto& buffers = *iov.MutableBuffers();
    buffers.Clear();
    buffers.Reserve(blocksCount);

    TSgList sglist;
    sglist.reserve(blocksCount);

    for (size_t i = 0; i < blocksCount; ++i) {
        auto& buffer = *buffers.Add();
        buffer.ReserveAndResize(blockSize);

        sglist.emplace_back(buffer.data(), buffer.size());
    }

    return sglist;
}

TSgList GetSgList(const NProto::TWriteBlocksRequest& request)
{
    const auto& iov = request.GetBlocks();
    TSgList sglist(Reserve(iov.BuffersSize()));

    for (const auto& buffer: iov.GetBuffers()) {
        if (buffer) {
            sglist.emplace_back(buffer.data(), buffer.size());
        } else {
            Y_DEBUG_ABORT_UNLESS(false, "write-request has empty buffer");
        }
    }

    return sglist;
}

TResultOrError<TSgList> GetSgList(
    const NProto::TReadBlocksResponse& response,
    ui32 expectedBlockSize)
{
    const auto& iov = response.GetBlocks();
    TSgList sglist(Reserve(iov.BuffersSize()));

    for (const auto& buffer: iov.GetBuffers()) {
        if (buffer) {
            if (buffer.size() != expectedBlockSize) {
                return MakeError(
                    E_ARGUMENT,
                    TStringBuilder() << "read-response has invalid buffer."
                                     << " BufferSize = " << buffer.size()
                                     << " BlockSize = " << expectedBlockSize);
            }
            sglist.emplace_back(buffer.data(), buffer.size());
        } else {
            sglist.emplace_back(
                TBlockDataRef::CreateZeroBlock(expectedBlockSize));
        }
    }

    return sglist;
}

TCopyStats CopyToSgList(
    const NProto::TIOVector& iov,
    const TSgList& sglist,
    ui64 offsetInBlocks,
    ui32 blockSize)
{
    TCopyStats result;

    ui64 offsetInBytes = offsetInBlocks * blockSize;

    // Skip offsetInBytes in destination
    size_t dstIndex = 0;
    for (; dstIndex != sglist.size(); ++dstIndex) {
        const size_t size = sglist[dstIndex].Size();

        Y_ABORT_UNLESS(size % blockSize == 0);

        if (offsetInBytes < size) {
            break;
        }
        offsetInBytes -= size;
    }

    for (const auto& src: iov.GetBuffers()) {
        Y_ABORT_UNLESS(src.size() == blockSize || src.size() == 0);
        Y_ABORT_UNLESS(dstIndex != sglist.size());

        TBlockDataRef dst = sglist[dstIndex];

        Y_ABORT_UNLESS(dst.Size() % blockSize == 0);
        Y_ABORT_UNLESS(dst.Size() - offsetInBytes >= blockSize);

        if (dst.Data()) {
            char* destBuff = const_cast<char*>(dst.Data()) + offsetInBytes;
            if (src.empty()) {
                memset(destBuff, 0, blockSize);
                ++result.VoidBlockCount;
            } else {
                memcpy(destBuff, src.data(), blockSize);
            }
        }

        offsetInBytes += blockSize;

        if (offsetInBytes == dst.Size()) {
            ++dstIndex;
            offsetInBytes = 0;
        }
    }

    result.TotalBlockCount = iov.GetBuffers().size();
    return result;
}

void TrimVoidBuffers(NProto::TIOVector& iov)
{
    for (auto& buffer: *iov.MutableBuffers()) {
        if (IsAllZeroes(buffer.data(), buffer.size())) {
            buffer.clear();
        }
    }
}

size_t CopyAndTrimVoidBuffers(
    TBlockDataRef src,
    ui32 blockCount,
    ui32 blockSize,
    NProto::TIOVector* iov)
{
    Y_ABORT_UNLESS(static_cast<size_t>(blockCount) * blockSize == src.Size());

    size_t bytesCount = 0;

    auto& buffers = *iov->MutableBuffers();
    buffers.Clear();
    buffers.Reserve(blockCount);

    const char* srcBuffer = src.Data();
    for (size_t i = 0; i < blockCount; ++i) {
        if (IsAllZeroes(srcBuffer, blockSize)) {
            // Add an empty buffer when data contains all zeros.
            buffers.Add();
        } else {
            buffers.Add()->assign(srcBuffer, blockSize);
        }

        srcBuffer += blockSize;
        bytesCount += blockSize;
    }

    return bytesCount;
}

ui32 CountVoidBuffers(const NProto::TIOVector& iov)
{
    ui32 result = 0;
    for (const auto& buffer: iov.GetBuffers()) {
        if (buffer.Empty()) {
            ++result;
        }
    }
    return result;
}

bool IsAllZeroes(const char* src, size_t size)
{
    using TBigNumber = ui64;

    if (size < sizeof(TBigNumber)) {
        for (size_t i = 0; i < size; ++i) {
            if (src[i] != 0) {
                return false;
            }
        }
        return true;
    }

    const bool isAligned =
        reinterpret_cast<std::uintptr_t>(src) % sizeof(TBigNumber) == 0;

    if (isAligned) {
        const TBigNumber* const a = reinterpret_cast<const TBigNumber*>(src);
        return !a[0] && !memcmp(
                            src,
                            src + sizeof(TBigNumber),
                            size - sizeof(TBigNumber));
    } else {
        return !src[0] && !memcmp(src, src + sizeof(char), size - sizeof(char));
    }
}

bool IsAllZeroes(TBlockDataRef block)
{
    return block.Data() == nullptr || IsAllZeroes(block.Data(), block.Size());
}

}   // namespace NCloud::NBlockStore
