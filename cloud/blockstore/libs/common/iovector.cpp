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
            Y_VERIFY_DEBUG(false, "write-request has empty buffer");
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
                return MakeError(E_ARGUMENT, TStringBuilder()
                    << "read-response has invalid buffer."
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

void CopyToSgList(
    const NProto::TIOVector& iov,
    const TSgList& sglist,
    ui64 offsetInBlocks,
    ui32 blockSize)
{
    ui64 offsetInBytes = offsetInBlocks * blockSize;

    size_t dstIndex = 0;
    for (; dstIndex != sglist.size(); ++dstIndex) {
        const size_t size = sglist[dstIndex].Size();

        Y_VERIFY(size % blockSize == 0);

        if (offsetInBytes < size) {
            break;
        }
        offsetInBytes -= size;
    }

    for (const auto& src: iov.GetBuffers()) {
        Y_VERIFY(src.size() == blockSize);
        Y_VERIFY(dstIndex != sglist.size());

        TBlockDataRef dst = sglist[dstIndex];

        Y_VERIFY(dst.Size() % blockSize == 0);
        Y_VERIFY(dst.Size() - offsetInBytes >= blockSize);

        memcpy(
            const_cast<char*>(dst.Data()) + offsetInBytes,
            src.data(),
            blockSize);

        offsetInBytes += blockSize;

        if (offsetInBytes == dst.Size()) {
            ++dstIndex;
            offsetInBytes = 0;
        }
    }
}

}   // namespace NCloud::NBlockStore
