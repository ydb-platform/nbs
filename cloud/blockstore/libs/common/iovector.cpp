#include "iovector.h"

#include <library/cpp/int128/int128.h>

#include <util/string/builder.h>

#include <immintrin.h>

#define IMPL 2
namespace NCloud::NBlockStore {

#if IMPL == 4
inline int TestAllZeros(__m256i x)
{
    return _mm256_testz_si256(x, x);
}
#endif
namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

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

TResultOrError<TSgList>
GetSgList(const NProto::TReadBlocksResponse& response, ui32 expectedBlockSize)
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
#if (IMPL == 1)
    using TBigNumber = ui64;
    Y_ABORT_UNLESS(size % sizeof(TBigNumber) == 0);

    const TBigNumber* const buffer = reinterpret_cast<const TBigNumber*>(src);
    for (size_t i = 0, n = size / sizeof(TBigNumber); i != n; ++i) {
        if (buffer[i] != 0) {
            return false;
        }
    }
    return true;
#endif
#if (IMPL == 2)
    using TBigNumber = ui64;
    Y_ABORT_UNLESS(size % sizeof(TBigNumber) == 0);

    const TBigNumber* const a = reinterpret_cast<const TBigNumber*>(src);
    if (*a) {
        return false;
    }
    return !memcmp(src, src + sizeof(TBigNumber), size - sizeof(TBigNumber));
#endif
#if (IMPL == 3)
    using TBigNumber = ui64;
    Y_ABORT_UNLESS(size % sizeof(TBigNumber) == 0);

    for (size_t i = 0; i != size; i += sizeof(TBigNumber)) {
        TBigNumber a = 0;
        std::memcpy(&a, src + i, sizeof(TBigNumber));
        if (a != 0) {
            return false;
        }
    }
    return true;
#endif
#if (IMPL == 4)
    using TBigNumber = __m256i;
    Y_ABORT_UNLESS(size % sizeof(TBigNumber) == 0);

    const TBigNumber* const buffer = reinterpret_cast<const TBigNumber*>(src);

    for (size_t i = 0, n = size / (sizeof(TBigNumber)); i != n; ++i) {
        if (!TestAllZeros(buffer[i])) {
            return false;
        }
    }
    return true;
#endif
}

}   // namespace NCloud::NBlockStore
