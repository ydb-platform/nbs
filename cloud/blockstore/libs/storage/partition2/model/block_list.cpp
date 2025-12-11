#include "block_list.h"

#include <util/generic/algorithm.h>
#include <util/generic/bitmap.h>
#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/system/align.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TListHeader
{
    static constexpr ui8 Blocks = 1;
    static constexpr ui8 DeletedBlocks = 2;

    ui32 ListType : 8;
    ui32 Unused : 24;

    TListHeader(ui8 listType)
        : ListType(listType)
        , Unused(0)
    {}
};

static_assert(sizeof(TListHeader) == 4, "");

struct TGroupHeader
{
    ui32 Step;
    ui32 Gen : 31;
    ui32 IsMulti : 1;

    TGroupHeader(ui32 gen, ui32 step, bool isMulti)
        : Step(step)
        , Gen(gen)
        , IsMulti(isMulti)
    {}
};

static_assert(sizeof(TGroupHeader) == 8, "");

struct TMultiGroupHeader
{
    static constexpr ui8 MergedGroup = 1;
    static constexpr ui8 MixedGroup = 2;

    ui32 GroupType : 8;
    ui32 Count : 16;
    ui32 WithZeroBlocks : 1;
    ui32 Unused : 7;

    TMultiGroupHeader(ui8 groupType, ui16 count, bool withZeroBlocks)
        : GroupType(groupType)
        , Count(count)
        , WithZeroBlocks(withZeroBlocks)
        , Unused(0)
    {}
};

static_assert(sizeof(TMultiGroupHeader) == 4, "");

////////////////////////////////////////////////////////////////////////////////

struct TBlockEntry
{
    ui32 BlockIndex;
    ui32 BlobOffset : 16;
    ui32 Zeroed : 1;
    ui32 Unused : 15;

    TBlockEntry(ui32 blockIndex, ui16 blobOffset, bool zeroed)
        : BlockIndex(blockIndex)
        , BlobOffset(blobOffset)
        , Zeroed(zeroed)
        , Unused(0)
    {}
};

static_assert(sizeof(TBlockEntry) == 8, "");

struct TSingleBlockEntry
    : TGroupHeader
    , TBlockEntry
{
    TSingleBlockEntry(
        ui32 gen,
        ui32 step,
        ui32 blockIndex,
        ui16 blobOffset,
        bool zeroed)
        : TGroupHeader(gen, step, false)
        , TBlockEntry(blockIndex, blobOffset, zeroed)
    {}
};

static_assert(sizeof(TSingleBlockEntry) == 8 + 8);

struct TMergedBlockEntry
    : TGroupHeader
    , TMultiGroupHeader
    , TBlockEntry
{
    TMergedBlockEntry(
        ui32 gen,
        ui32 step,
        ui16 count,
        ui32 blockIndex,
        ui16 blobOffset,
        bool zeroed)
        : TGroupHeader(gen, step, true)
        , TMultiGroupHeader(TMultiGroupHeader::MergedGroup, count, true)
        , TBlockEntry(blockIndex, blobOffset, zeroed)
    {}
};

static_assert(sizeof(TMergedBlockEntry) == 8 + 4 + 8);

struct TMixedBlockEntry
    : TGroupHeader
    , TMultiGroupHeader
{
    TMixedBlockEntry(ui32 gen, ui32 step, ui16 count, bool withZeroBlocks)
        : TGroupHeader(gen, step, true)
        , TMultiGroupHeader(
              TMultiGroupHeader::MixedGroup,
              count,
              withZeroBlocks)
    {}
};

static_assert(sizeof(TMixedBlockEntry) == 8 + 4);

////////////////////////////////////////////////////////////////////////////////

struct TDeletedBlockEntry
{
    ui32 BlobOffset : 16;
    ui32 Unused : 16;

    TDeletedBlockEntry(ui16 blobOffset)
        : BlobOffset(blobOffset)
        , Unused(0)
    {}
};

static_assert(sizeof(TDeletedBlockEntry) == 4);

struct TSingleDeletedBlockEntry
    : TGroupHeader
    , TDeletedBlockEntry
{
    TSingleDeletedBlockEntry(ui32 gen, ui32 step, ui16 blobOffset)
        : TGroupHeader(gen, step, false)
        , TDeletedBlockEntry(blobOffset)
    {}
};

static_assert(sizeof(TSingleDeletedBlockEntry) == 8 + 4);

struct TMergedDeletedBlockEntry
    : TGroupHeader
    , TMultiGroupHeader
    , TDeletedBlockEntry
{
    TMergedDeletedBlockEntry(ui32 gen, ui32 step, ui16 count, ui16 blobOffset)
        : TGroupHeader(gen, step, true)
        , TMultiGroupHeader(TMultiGroupHeader::MergedGroup, count, false)
        , TDeletedBlockEntry(blobOffset)
    {}
};

static_assert(sizeof(TMergedDeletedBlockEntry) == 8 + 4 + 4);

struct TMixedDeletedBlockEntry
    : TGroupHeader
    , TMultiGroupHeader
{
    TMixedDeletedBlockEntry(ui32 gen, ui32 step, ui16 count)
        : TGroupHeader(gen, step, true)
        , TMultiGroupHeader(TMultiGroupHeader::MixedGroup, count, false)
    {}
};

static_assert(sizeof(TMixedDeletedBlockEntry) == 8 + 4);

////////////////////////////////////////////////////////////////////////////////

class TBinaryWriter: public IOutputStream
{
private:
    TByteVector Buffer{{GetAllocatorByTag(EAllocatorTag::BlobIndexBlockList)}};

public:
    size_t Written() const
    {
        return Buffer.size();
    }

    void Write(const char* buf, size_t count)
    {
        memcpy(Grow(count), buf, count);
    }

    template <typename T>
    void Write(const T& val)
    {
        memcpy(Grow(sizeof(val)), &val, sizeof(val));
    }

    TByteVector Finish()
    {
        return std::move(Buffer);
    }

protected:
    // IOutputStream:
    void DoWrite(const void* buf, size_t count) override
    {
        return Write((char*)buf, count);
    }

private:
    char* Grow(size_t count)
    {
        size_t offset = Buffer.size();
        Buffer.resize(offset + count);
        return (char*)Buffer.data() + offset;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBinaryReader: public IInputStream
{
private:
    const TByteVector& Buffer;
    TByteVector::const_iterator Ptr = Buffer.begin();

public:
    TBinaryReader(const TByteVector& buffer)
        : Buffer(buffer)
    {}

    size_t Avail() const
    {
        return std::distance(Ptr, Buffer.end());
    }

    const char* Read(size_t count)
    {
        return Consume(count);
    }

    template <typename T>
    const T& Read()
    {
        return *reinterpret_cast<const T*>(Consume(sizeof(T)));
    }

    template <typename T>
    const T* Read(size_t count)
    {
        return reinterpret_cast<const T*>(Consume(count * sizeof(T)));
    }

protected:
    // IInputStream:
    size_t DoRead(void* buf, size_t count) override
    {
        const auto* read = Read(count);
        memcpy(buf, read, count);
        return count;
    }

private:
    const char* Consume(size_t count)
    {
        Y_ABORT_UNLESS(Avail() >= count, "Invalid encoding");
        const char* p = Ptr;
        Ptr += count;
        return p;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T Align2(T count)
{
    return AlignUp<T>(count, 2);
}

template <typename T>
size_t FindOffset(const T* begin, const T* end, T item)
{
    static constexpr size_t LinearSearchLimit = 10;

    if (begin + LinearSearchLimit > end) {
        const T* it = std::find(begin, end, item);
        if (it != end) {
            return it - begin;
        }
    } else {
        const T* it = std::lower_bound(begin, end, item);
        if (it != end && *it == item) {
            return it - begin;
        }
    }

    return NPOS;
}

void ReadBitMap(TBinaryReader& reader, TDynBitMap& mask)
{
    auto read = reader.Avail();
    mask.Load(&reader);
    read -= reader.Avail();

    if (read % 4 != 0) {
        auto padding = 4 - read % 4;
        for (size_t i = 0; i < padding; ++i) {
            reader.Read<ui8>();
        }
    }
}

void WriteBitMap(TBinaryWriter& writer, const TDynBitMap& mask)
{
    auto written = writer.Written();
    mask.Save(&writer);
    written = writer.Written() - written;

    if (written % 4 != 0) {
        auto padding = 4 - written % 4;
        for (size_t i = 0; i < padding; ++i) {
            writer.Write<ui8>(0);   // padding
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TBlockList::TBlockBuilder
{
private:
    TVector<TBlock> Blocks{MaxBlocksCount};
    size_t BlocksCount = 0;

public:
    void
    AddBlock(ui16 blobOffset, ui32 blockIndex, ui64 minCommitId, bool zeroed)
    {
        Y_ABORT_UNLESS(blobOffset < MaxBlocksCount, "Invalid encoding");
        Blocks[blobOffset] =
            TBlock(blockIndex, minCommitId, InvalidCommitId, zeroed);
        ++BlocksCount;
    }

    void AddDeletedBlock(ui16 blobOffset, ui64 maxCommitId)
    {
        Y_ABORT_UNLESS(blobOffset < BlocksCount, "Invalid encoding");
        Blocks[blobOffset].MaxCommitId = maxCommitId;
    }

    TVector<TBlock> Finish()
    {
        Blocks.resize(BlocksCount);
        return std::move(Blocks);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBlockList::TDeletedBlockBuilder
{
private:
    TVector<TDeletedBlockMark> Blocks;

public:
    void AddDeletedBlock(ui16 blobOffset, ui64 maxCommitId)
    {
        Blocks.emplace_back(blobOffset, maxCommitId);
    }

    TVector<TDeletedBlockMark> Finish()
    {
        return std::move(Blocks);
    }
};

////////////////////////////////////////////////////////////////////////////////

TMaybe<TBlockMark> TBlockList::FindBlock(
    ui32 blockIndex,
    ui64 maxCommitId) const
{
    TBinaryReader reader(Blocks);

    const auto& list = reader.Read<TListHeader>();
    Y_ABORT_UNLESS(list.ListType == TListHeader::Blocks);

    while (reader.Avail()) {
        const auto& group = reader.Read<TGroupHeader>();
        ui64 groupCommitId = MakeCommitId(group.Gen, group.Step);

        if (!group.IsMulti) {
            // single block
            const auto& entry = reader.Read<TBlockEntry>();
            if (groupCommitId <= maxCommitId && entry.BlockIndex == blockIndex)
            {
                return TBlockMark(
                    entry.BlobOffset,
                    entry.BlockIndex,
                    groupCommitId,
                    entry.Zeroed);
            }
        } else {
            const auto& multi = reader.Read<TMultiGroupHeader>();
            switch (multi.GroupType) {
                // merged blocks
                case TMultiGroupHeader::MergedGroup: {
                    const auto& entry = reader.Read<TBlockEntry>();
                    if (groupCommitId <= maxCommitId &&
                        entry.BlockIndex <= blockIndex)
                    {
                        size_t i = blockIndex - entry.BlockIndex;
                        if (i < multi.Count) {
                            return TBlockMark(
                                entry.BlobOffset + i,
                                entry.BlockIndex + i,
                                groupCommitId,
                                entry.Zeroed);
                        }
                    }
                    break;
                }

                // mixed blocks
                case TMultiGroupHeader::MixedGroup: {
                    const auto* blockIndices = reader.Read<ui32>(multi.Count);
                    const auto* blobOffsets =
                        reader.Read<ui16>(Align2(multi.Count));

                    TDynBitMap zeroed;
                    if (multi.WithZeroBlocks) {
                        ReadBitMap(reader, zeroed);
                    }

                    if (groupCommitId <= maxCommitId) {
                        size_t i = FindOffset(
                            blockIndices,
                            blockIndices + multi.Count,
                            blockIndex);
                        if (i != NPOS) {
                            return TBlockMark(
                                blobOffsets[i],
                                blockIndices[i],
                                groupCommitId,
                                zeroed[i]);
                        }
                    }
                    break;
                }

                default:
                    Y_ABORT();
            }
        }
    }

    return Nothing();
}

ui64 TBlockList::FindDeletedBlock(ui16 blobOffset) const
{
    TBinaryReader reader(DeletedBlocks);

    const auto& list = reader.Read<TListHeader>();
    Y_ABORT_UNLESS(list.ListType == TListHeader::DeletedBlocks);

    while (reader.Avail()) {
        const auto& group = reader.Read<TGroupHeader>();
        ui64 groupCommitId = MakeCommitId(group.Gen, group.Step);

        if (!group.IsMulti) {
            // single block
            const auto& entry = reader.Read<TDeletedBlockEntry>();
            if (entry.BlobOffset == blobOffset) {
                return groupCommitId;
            }
        } else {
            const auto& multi = reader.Read<TMultiGroupHeader>();
            switch (multi.GroupType) {
                // merged blocks
                case TMultiGroupHeader::MergedGroup: {
                    const auto& entry = reader.Read<TDeletedBlockEntry>();
                    if (entry.BlobOffset <= blobOffset) {
                        size_t i = blobOffset - entry.BlobOffset;
                        if (i < multi.Count) {
                            return groupCommitId;
                        }
                    }
                    break;
                }

                // mixed blocks
                case TMultiGroupHeader::MixedGroup: {
                    const auto* blobOffsets =
                        reader.Read<ui16>(Align2(multi.Count));
                    size_t i = FindOffset(
                        blobOffsets,
                        blobOffsets + multi.Count,
                        blobOffset);
                    if (i != NPOS) {
                        return groupCommitId;
                    }
                    break;
                }

                default:
                    Y_ABORT();
            }
        }
    }

    return InvalidCommitId;
}

////////////////////////////////////////////////////////////////////////////////

TVector<TBlock> TBlockList::GetBlocks() const
{
    TBlockList::TBlockBuilder builder;
    DecodeBlocks(builder);
    DecodeDeletedBlocks(builder);
    return builder.Finish();
}

TVector<TDeletedBlockMark> TBlockList::GetDeletedBlocks() const
{
    TBlockList::TDeletedBlockBuilder builder;
    DecodeDeletedBlocks(builder);
    return builder.Finish();
}

ui32 TBlockList::CountBlocks() const
{
    ui32 count = 0;

    TBinaryReader reader(Blocks);

    const auto& list = reader.Read<TListHeader>();
    Y_ABORT_UNLESS(list.ListType == TListHeader::Blocks);

    while (reader.Avail()) {
        const auto& group = reader.Read<TGroupHeader>();

        if (!group.IsMulti) {
            // single block
            reader.Read<TBlockEntry>();
            ++count;
        } else {
            const auto& multi = reader.Read<TMultiGroupHeader>();
            count += multi.Count;

            switch (multi.GroupType) {
                // merged blocks
                case TMultiGroupHeader::MergedGroup: {
                    reader.Read<TBlockEntry>();
                    break;
                }

                // mixed blocks
                case TMultiGroupHeader::MixedGroup: {
                    reader.Read<ui32>(multi.Count);
                    reader.Read<ui16>(Align2(multi.Count));
                    if (multi.WithZeroBlocks) {
                        TDynBitMap zeroed;
                        ReadBitMap(reader, zeroed);
                    }
                    break;
                }

                default:
                    Y_ABORT();
            }
        }
    }

    return count;
}

template <typename T>
void TBlockList::DecodeBlocks(T& builder) const
{
    TBinaryReader reader(Blocks);

    const auto& list = reader.Read<TListHeader>();
    Y_ABORT_UNLESS(list.ListType == TListHeader::Blocks);

    while (reader.Avail()) {
        const auto& group = reader.Read<TGroupHeader>();
        ui64 groupCommitId = MakeCommitId(group.Gen, group.Step);

        if (!group.IsMulti) {
            // single block
            const auto& entry = reader.Read<TBlockEntry>();
            builder.AddBlock(
                entry.BlobOffset,
                entry.BlockIndex,
                groupCommitId,
                entry.Zeroed);
        } else {
            const auto& multi = reader.Read<TMultiGroupHeader>();
            switch (multi.GroupType) {
                // merged blocks
                case TMultiGroupHeader::MergedGroup: {
                    const auto& entry = reader.Read<TBlockEntry>();
                    for (size_t i = 0; i < multi.Count; ++i) {
                        builder.AddBlock(
                            entry.BlobOffset + i,
                            entry.BlockIndex + i,
                            groupCommitId,
                            entry.Zeroed);
                    }
                    break;
                }

                // mixed blocks
                case TMultiGroupHeader::MixedGroup: {
                    const auto* blockIndices = reader.Read<ui32>(multi.Count);
                    const auto* blobOffsets =
                        reader.Read<ui16>(Align2(multi.Count));

                    TDynBitMap zeroed;
                    if (multi.WithZeroBlocks) {
                        ReadBitMap(reader, zeroed);
                    }

                    for (size_t i = 0; i < multi.Count; ++i) {
                        builder.AddBlock(
                            blobOffsets[i],
                            blockIndices[i],
                            groupCommitId,
                            zeroed[i]);
                    }
                    break;
                }

                default:
                    Y_ABORT();
            }
        }
    }
}

template <typename T>
void TBlockList::DecodeDeletedBlocks(T& builder) const
{
    TBinaryReader reader(DeletedBlocks);

    const auto& list = reader.Read<TListHeader>();
    Y_ABORT_UNLESS(list.ListType == TListHeader::DeletedBlocks);

    while (reader.Avail()) {
        const auto& group = reader.Read<TGroupHeader>();
        ui64 groupCommitId = MakeCommitId(group.Gen, group.Step);

        if (!group.IsMulti) {
            // single entry
            const auto& entry = reader.Read<TDeletedBlockEntry>();
            builder.AddDeletedBlock(entry.BlobOffset, groupCommitId);
        } else {
            const auto& multi = reader.Read<TMultiGroupHeader>();
            switch (multi.GroupType) {
                // merged blocks
                case TMultiGroupHeader::MergedGroup: {
                    const auto& entry = reader.Read<TDeletedBlockEntry>();
                    for (size_t i = 0; i < multi.Count; ++i) {
                        builder.AddDeletedBlock(
                            entry.BlobOffset + i,
                            groupCommitId);
                    }
                    break;
                }

                // mixed blocks
                case TMultiGroupHeader::MixedGroup: {
                    const auto* blobOffsets =
                        reader.Read<ui16>(Align2(multi.Count));
                    for (size_t i = 0; i < multi.Count; ++i) {
                        builder.AddDeletedBlock(blobOffsets[i], groupCommitId);
                    }
                    break;
                }

                default:
                    Y_ABORT();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TBlockList TBlockListBuilder::Finish()
{
    return {EncodeBlocks(), EncodeDeletedBlocks()};
}

TByteVector TBlockListBuilder::EncodeBlocks()
{
    Sort(
        Blocks,
        [](const auto& l, const auto& r)
        {
            // order by (MinCommitId DESC, BlockIndex ASC)
            return l.MinCommitId > r.MinCommitId ||
                   (l.MinCommitId == r.MinCommitId &&
                    l.BlockIndex < r.BlockIndex);
        });

    TBinaryWriter writer;
    writer.Write<TListHeader>({TListHeader::Blocks});

    auto isMerged = [&](size_t start, size_t end)
    {
        for (size_t i = start + 1; i < end; ++i) {
            if (Blocks[i].BlockIndex != Blocks[i - 1].BlockIndex + 1 ||
                Blocks[i].BlobOffset != Blocks[i - 1].BlobOffset + 1 ||
                Blocks[i].Zeroed != Blocks[i - 1].Zeroed)
            {
                return false;
            }
        }
        return true;
    };

    auto writeBlocks = [&](size_t start, size_t end, ui64 commitId)
    {
        size_t count = end - start;
        Y_ABORT_UNLESS(count <= MaxBlocksCount);

        ui32 gen, step;
        std::tie(gen, step) = ParseCommitId(commitId);

        if (count == 1) {
            writer.Write<TSingleBlockEntry>(
                {gen,
                 step,
                 Blocks[start].BlockIndex,
                 Blocks[start].BlobOffset,
                 Blocks[start].Zeroed});
        } else if (count > 1) {
            if (isMerged(start, end)) {
                writer.Write<TMergedBlockEntry>(
                    {gen,
                     step,
                     static_cast<ui16>(count),
                     Blocks[start].BlockIndex,
                     Blocks[start].BlobOffset,
                     Blocks[start].Zeroed});
            } else {
                bool withZeroBlocks = FindIf(
                    Blocks.begin() + start,
                    Blocks.begin() + end,
                    [](const auto& b) { return b.Zeroed; });
                writer.Write<TMixedBlockEntry>(
                    {gen, step, static_cast<ui16>(count), withZeroBlocks});
                for (size_t i = start; i < end; ++i) {
                    writer.Write<ui32>(Blocks[i].BlockIndex);
                }
                for (size_t i = start; i < end; ++i) {
                    writer.Write<ui16>(Blocks[i].BlobOffset);
                }
                if (count & 1) {
                    writer.Write<ui16>(0);   // padding
                }
                if (withZeroBlocks) {
                    TDynBitMap zeroed;
                    for (size_t i = start; i < end; ++i) {
                        if (Blocks[i].Zeroed) {
                            zeroed.Set(i - start);
                        }
                    }
                    WriteBitMap(writer, zeroed);
                }
            }
        }
    };

    size_t groupStart = 0;
    size_t groupEnd = 0;
    ui64 groupCommitId = 0;

    for (size_t i = 0; i < Blocks.size(); ++i) {
        if (groupCommitId == Blocks[i].MinCommitId) {
            ++groupEnd;
        } else {
            writeBlocks(groupStart, groupEnd, groupCommitId);

            groupCommitId = Blocks[i].MinCommitId;
            groupStart = i;
            groupEnd = i + 1;
        }
    }

    writeBlocks(groupStart, groupEnd, groupCommitId);
    return writer.Finish();
}

TByteVector TBlockListBuilder::EncodeDeletedBlocks()
{
    Sort(
        DeletedBlocks,
        [](const auto& l, const auto& r)
        {
            // order by (MaxCommitId DESC, BlobOffset ASC)
            return l.MaxCommitId > r.MaxCommitId ||
                   (l.MaxCommitId == r.MaxCommitId &&
                    l.BlobOffset < r.BlobOffset);
        });

    TBinaryWriter writer;
    writer.Write<TListHeader>({TListHeader::DeletedBlocks});

    auto isMerged = [&](size_t start, size_t end)
    {
        for (size_t i = start + 1; i < end; ++i) {
            if (DeletedBlocks[i].BlobOffset !=
                DeletedBlocks[i - 1].BlobOffset + 1)
            {
                return false;
            }
        }
        return true;
    };

    auto writeBlocks = [&](size_t start, size_t end, ui64 commitId)
    {
        size_t count = end - start;
        Y_ABORT_UNLESS(count <= MaxBlocksCount);

        ui32 gen, step;
        std::tie(gen, step) = ParseCommitId(commitId);

        if (count == 1) {
            writer.Write<TSingleDeletedBlockEntry>(
                {gen, step, DeletedBlocks[start].BlobOffset});
        } else if (count > 1) {
            if (isMerged(start, end)) {
                writer.Write<TMergedDeletedBlockEntry>(
                    {gen,
                     step,
                     static_cast<ui16>(count),
                     DeletedBlocks[start].BlobOffset});
            } else {
                writer.Write<TMixedDeletedBlockEntry>(
                    {gen, step, static_cast<ui16>(count)});
                for (size_t i = start; i < end; ++i) {
                    writer.Write<ui16>(DeletedBlocks[i].BlobOffset);
                }
                if (count & 1) {
                    writer.Write<ui16>(0);   // padding
                }
            }
        }
    };

    size_t groupStart = 0;
    size_t groupEnd = 0;
    ui64 groupCommitId = 0;

    for (size_t i = 0; i < DeletedBlocks.size(); ++i) {
        if (groupCommitId == DeletedBlocks[i].MaxCommitId) {
            ++groupEnd;
        } else {
            writeBlocks(groupStart, groupEnd, groupCommitId);

            groupCommitId = DeletedBlocks[i].MaxCommitId;
            groupStart = i;
            groupEnd = i + 1;
        }
    }

    writeBlocks(groupStart, groupEnd, groupCommitId);
    return writer.Finish();
}

////////////////////////////////////////////////////////////////////////////////

TBlockList BuildBlockList(const TVector<TBlock>& blocks)
{
    TBlockListBuilder builder;

    ui16 blobOffset = 0;
    for (const auto& block: blocks) {
        builder.AddBlock(
            blobOffset,
            block.BlockIndex,
            block.MinCommitId,
            block.Zeroed);

        if (block.MaxCommitId != InvalidCommitId) {
            builder.AddDeletedBlock(blobOffset, block.MaxCommitId);
        }

        ++blobOffset;
    }

    return builder.Finish();
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
