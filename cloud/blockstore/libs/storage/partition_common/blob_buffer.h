#pragma once

#include <cloud/storage/core/libs/common/block_buffer.h>
#include <cloud/storage/core/libs/common/sglist.h>

#include <contrib/ydb/library/actors/util/rc_buf.h>

#include <util/generic/ptr.h>
#include <util/generic/ylimits.h>

namespace NCloud::NBlockStore::NStorage {

namespace NBlobBufferDetail {

// Keep the allocator and its actual allocation together through all TRcBuf
// references, including Clone() performed by the BlobStorage buffer machinery.
class TAllocation final: public ::IContiguousChunk
{
private:
    IAllocator* const Allocator;
    const IAllocator::TBlock Allocation;
    const size_t Size;

public:
    TAllocation(IAllocator* allocator, size_t size)
        : Allocator(allocator)
        , Allocation(allocator->Allocate(size))
        , Size(size)
    {
        Y_ABORT_UNLESS(Allocation.Data && Allocation.Len >= Size);
        memset(Allocation.Data, 0, Size);
    }

    ~TAllocation() override
    {
        Allocator->Release(Allocation);
    }

    ::TContiguousSpan GetData() const override
    {
        return {static_cast<const char*>(Allocation.Data), Size};
    }

    ::TMutableContiguousSpan UnsafeGetDataMut() override
    {
        return {static_cast<char*>(Allocation.Data), Size};
    }

    size_t GetOccupiedMemorySize() const override
    {
        return Allocation.Len;
    }

    ::IContiguousChunk::TPtr Clone() override
    {
        auto clone = MakeIntrusive<TAllocation>(Allocator, Size);
        memcpy(clone->Allocation.Data, Allocation.Data, Size);
        return clone;
    }
};

}   // namespace NBlobBufferDetail

// A final-size contiguous buffer for ordinary flush/compaction, with the old
// block-by-block representation retained for patching. SG entries never own
// allocations; the TRcBuf backend is the single owner of a contiguous payload.
class TBlobBuffer
{
private:
    TBlockBuffer Legacy;
    ::TRcBuf Content;
    TVector<TBlockDataRef> Blocks;
    char* Data = nullptr;
    ui32 BlockSize = 0;
    size_t Capacity = 0;

public:
    TBlobBuffer(IAllocator* allocator = TDefaultAllocator::Instance())
        : Legacy(allocator)
    {}

    // Compatibility for callers that deliberately retain the legacy path.
    TBlobBuffer(TBlockBuffer legacy)
        : Legacy(std::move(legacy))
    {}

    TBlobBuffer(TBlobBuffer&&) = default;
    TBlobBuffer& operator=(TBlobBuffer&&) = default;

    static TBlobBuffer Allocate(
        ui32 blockSize,
        ui32 blocksCount,
        IAllocator* allocator)
    {
        Y_ABORT_UNLESS(blockSize);
        Y_ABORT_UNLESS(blocksCount <= Max<size_t>() / blockSize);
        TBlobBuffer result(allocator);
        if (!blocksCount) {
            return result;
        }
        result.BlockSize = blockSize;
        result.Capacity = blocksCount;
        result.Blocks.reserve(blocksCount);
        ::IContiguousChunk::TPtr allocation =
            MakeIntrusive<NBlobBufferDetail::TAllocation>(
                allocator,
                size_t(blockSize) * blocksCount);
        result.Data = allocation->UnsafeGetDataMut().data();
        result.Content = ::TRcBuf(std::move(allocation));
        return result;
    }

    bool IsContiguous() const
    {
        return Content.GetData() != nullptr;
    }

    explicit operator bool() const
    {
        return GetBlocksCount() != 0;
    }

    size_t GetBlocksCount() const
    {
        return IsContiguous() ? Blocks.size() : Legacy.GetBlocksCount();
    }

    size_t GetBytesCount() const
    {
        return IsContiguous() ? Blocks.size() * BlockSize : Legacy.GetBytesCount();
    }

    const TVector<TBlockDataRef>& GetBlocks() const
    {
        return IsContiguous() ? Blocks : Legacy.GetBlocks();
    }

    TBlockDataRef GetBlock(size_t index) const
    {
        Y_ABORT_UNLESS(index < GetBlocksCount());
        return GetBlocks()[index];
    }

    void AddBlock(TBlockDataRef block)
    {
        if (!IsContiguous()) {
            Legacy.AddBlock(block);
            return;
        }
        Y_ABORT_UNLESS(block.Data());
        memcpy(AppendBlock(block.Size()), block.Data(), block.Size());
    }

    void AddBlock(size_t size, char fill)
    {
        if (!IsContiguous()) {
            Legacy.AddBlock(size, fill);
            return;
        }
        char* data = AppendBlock(size);
        // Newly appended slots have not been exposed yet and the whole
        // allocation is already zeroed. Preserve the zero tail used by Repair.
        if (fill) {
            memset(data, fill, size);
        }
    }

    void AddZeroBlock(size_t size)
    {
        if (IsContiguous()) {
            AddBlock(size, 0);
        } else {
            Legacy.AddZeroBlock(size);
        }
    }

    TString AsString() const
    {
        return IsContiguous()
            ? TString(Content.GetData(), GetBytesCount())
            : Legacy.AsString();
    }

    // All writes, checksum verification and diagnostic consumers must finish
    // before sharing. The producer retains its guard/cancellation gate while
    // each submitted event receives a real immutable owner of the same bytes.
    ::TRcBuf ShareContent() const
    {
        Y_ABORT_UNLESS(IsContiguous() && Blocks.size() == Capacity);
        return Content;
    }

private:
    char* AppendBlock(size_t size)
    {
        Y_ABORT_UNLESS(size == BlockSize && Blocks.size() < Capacity);
        char* data = Data + Blocks.size() * BlockSize;
        Blocks.emplace_back(data, size);
        return data;
    }
};

inline TSgList CreateSgList(const TBlobBuffer& buffer)
{
    return buffer.GetBlocks();
}

}   // namespace NCloud::NBlockStore::NStorage
