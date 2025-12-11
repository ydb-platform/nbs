#include <cloud/blockstore/libs/storage/partition2/model/block_list.h>

#include <library/cpp/archive/yarchive.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/benchmark/bench.h>

#include <util/generic/vector.h>
#include <util/memory/blob.h>
#include <util/random/fast.h>
#include <util/string/printf.h>

using namespace NCloud;
using namespace NCloud::NBlockStore;
using namespace NCloud::NBlockStore::NStorage;
using namespace NCloud::NBlockStore::NStorage::NPartition2;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const ui8 TEST_DATA[] = {
#include "data.inc"
};

static TArchiveReader ArchiveReader(
    TBlob::NoCopy(TEST_DATA, sizeof(TEST_DATA)));

TVector<TBlockList> ReadBlockLists(const TString& key)
{
    TVector<TBlockList> blockLists;

    auto stream = ArchiveReader.ObjectByKey(Sprintf("/%s.json", key.c_str()));
    NJson::TJsonValue v;
    NJson::ReadJsonTree(stream->ReadAll(), &v, true);
    const auto& arr = v["BlockLists"]["List"].GetArraySafe();

    for (const auto& x: arr) {
        const auto& encodedBlocks = x["Blocks"].GetStringSafe();
        const auto& encodedDeletedBlocks = x["DeletedBlocks"].GetStringSafe();

        auto blocks = FromStringBuf(
            Base64Decode(encodedBlocks),
            TDefaultAllocator::Instance());
        auto deletedBlocks = FromStringBuf(
            Base64Decode(encodedDeletedBlocks),
            TDefaultAllocator::Instance());

        blockLists.emplace_back(std::move(blocks), std::move(deletedBlocks));
    }

    return blockLists;
}

////////////////////////////////////////////////////////////////////////////////

TFastRng<ui32> Random(12345);

TVector<TBlock> GetSeqBlocks(size_t blocksCount = MaxBlocksCount)
{
    ui32 blockIndex = 123456;
    ui64 commitId = MakeCommitId(12, 345);

    TVector<TBlock> blocks(Reserve(blocksCount));
    for (size_t i = 0; i < blocksCount; ++i) {
        blocks.emplace_back(blockIndex + i, commitId, InvalidCommitId, false);
    }

    return blocks;
}

TVector<TBlock> GetRandomBlocks(size_t blocksCount = MaxBlocksCount)
{
    ui32 blockIndex = 123456;
    ui64 commitId = MakeCommitId(12, 345);

    TVector<TBlock> blocks(Reserve(blocksCount));
    for (size_t i = 0; i < blocksCount; ++i) {
        blocks.emplace_back(
            blockIndex + Random.Uniform(10000),
            commitId + Random.Uniform(10),
            InvalidCommitId,
            false);
    }

    return blocks;
}

////////////////////////////////////////////////////////////////////////////////

void EncodeBlockList(
    const TVector<TBlock>& blocks,
    const NBench::NCpu::TParams& iface)
{
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        auto list = BuildBlockList(blocks);
        Y_DO_NOT_OPTIMIZE_AWAY(&list);
    }
}

void DecodeBlockList(
    const TVector<TBlock>& blocks,
    const NBench::NCpu::TParams& iface)
{
    auto list = BuildBlockList(blocks);

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        Y_DO_NOT_OPTIMIZE_AWAY(list.GetBlocks());
    }
}

void DecodeBlockLists(
    const TVector<TBlockList>& blockLists,
    const NBench::NCpu::TParams& iface)
{
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        const auto& list = blockLists[i % blockLists.size()];
        Y_DO_NOT_OPTIMIZE_AWAY(list.GetBlocks());
    }
}

void FindBlock(
    const TVector<TBlock>& blocks,
    const NBench::NCpu::TParams& iface)
{
    auto list = BuildBlockList(blocks);

    size_t index = 0;
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        const auto& block = blocks[index];
        if (++index == blocks.size()) {
            index = 0;
        }

        Y_DO_NOT_OPTIMIZE_AWAY(list.FindBlock(block.BlockIndex));
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

static const auto SeqBlocks = GetSeqBlocks();

Y_CPU_BENCHMARK(TBlockList_Encode_SeqBlocks, iface)
{
    EncodeBlockList(SeqBlocks, iface);
}

Y_CPU_BENCHMARK(TBlockList_Decode_SeqBlocks, iface)
{
    DecodeBlockList(SeqBlocks, iface);
}

Y_CPU_BENCHMARK(TBlockList_FindBlock_SeqBlocks, iface)
{
    FindBlock(SeqBlocks, iface);
}

////////////////////////////////////////////////////////////////////////////////

static const auto RandomBlocks = GetRandomBlocks();

Y_CPU_BENCHMARK(TBlockList_Encode_RandomBlocks, iface)
{
    EncodeBlockList(RandomBlocks, iface);
}

Y_CPU_BENCHMARK(TBlockList_Decode_RandomBlocks, iface)
{
    DecodeBlockList(RandomBlocks, iface);
}

Y_CPU_BENCHMARK(TBlockList_FindBlock_RandomBlocks, iface)
{
    FindBlock(RandomBlocks, iface);
}

////////////////////////////////////////////////////////////////////////////////

static const auto SavedBlockLists = ReadBlockLists("blocklists");

Y_CPU_BENCHMARK(TBlockList_Decode_SavedBlockLists, iface)
{
    DecodeBlockLists(SavedBlockLists, iface);
}
