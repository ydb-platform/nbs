#include "blob_compressor.h"

#include <library/cpp/blockcodecs/codecs.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/array_ref.h>
#include <util/generic/cast.h>
#include <util/random/fast.h>
#include <util/string/cast.h>
#include <util/system/unaligned_mem.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 TabletId = 123456789;
constexpr ui32 BlockSize = 4096;
constexpr ui32 ChunkSize = 32 * 1024;

NKikimr::TLogoBlobID MakeBlobId(const NProto::TBlobCompression& compression)
{
    ui64 size = 0;
    for (const auto chunkSize: compression.GetChunkSizes()) {
        size += chunkSize;
    }
    return NKikimr::TLogoBlobID(
        TabletId,
        1,
        2,
        3,
        SafeIntegerCast<ui32>(size),
        4);
}

const NBlockCodecs::ICodec* GetCodec(NProto::TBlobCompression::ECodec codec)
{
    return NBlockCodecs::Codec(
        codec == NProto::TBlobCompression::LZ4 ? "lz4" : "zstd_1");
}

TString DecompressBlob(
    const TBlobCompressor& compressor,
    const NProto::TBlobCompression& compression,
    TStringBuf data,
    size_t size)
{
    TString result(size, '?');
    TSgList outputs;
    TVector<ui16> blocks;
    char* dest = result.begin();
    for (size_t i = 0; i < size / BlockSize; ++i) {
        outputs.push_back({dest + i * BlockSize, BlockSize});
        blocks.push_back(static_cast<ui16>(i));
    }
    UNIT_ASSERT(!HasError(compressor.DecompressBlob(
        {TRope(TString(data))},
        outputs,
        compression,
        blocks,
        nullptr)));
    return result;
}

template <typename T>
T GetResultOrFail(TResultOrError<T> result)
{
    UNIT_ASSERT_C(!HasError(result.GetError()), FormatError(result.GetError()));
    return result.ExtractResult();
}

template <typename TAction>
void AssertArgumentError(const TAction& action)
{
    const auto error = action();
    UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), FormatError(error));
    UNIT_ASSERT_STRING_CONTAINS(error.GetMessage(), ToString(TabletId));
}

NProto::TBlobCompression MakeCompression()
{
    NProto::TBlobCompression compression;
    compression.SetCodec(NProto::TBlobCompression::LZ4);
    compression.SetChunkSize(ChunkSize);
    for (ui32 size: {100, 200, 300, 400, 500}) {
        compression.AddChunkSizes(size);
    }
    return compression;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlobCompressorTest)
{
    Y_UNIT_TEST(ShouldReturnCompressedChunksAndMetadata)
    {
        for (const ui32 chunkSize: {16 * 1024, 64 * 1024}) {
            for (
                const auto codec:
                {NProto::TBlobCompression::LZ4, NProto::TBlobCompression::ZSTD})
            {
                const TBlobCompressor compressor(
                    TabletId,
                    BlockSize,
                    chunkSize,
                    codec);
                // Two complete chunks followed by one block.
                const TString payload = TString(chunkSize, 'a') +
                                        TString(chunkSize, 'b') +
                                        TString(BlockSize, 'c');
                const auto [compressed, compression] =
                    GetResultOrFail(compressor.CompressBlob(payload));

                UNIT_ASSERT(codec == compression.GetCodec());
                UNIT_ASSERT_VALUES_EQUAL(chunkSize, compression.GetChunkSize());
                UNIT_ASSERT_VALUES_EQUAL(3, compression.ChunkSizesSize());
                UNIT_ASSERT(compressed.size() < payload.size());
                UNIT_ASSERT_VALUES_EQUAL(
                    payload,
                    DecompressBlob(
                        compressor,
                        compression,
                        compressed,
                        payload.size()));

                ui64 offset = 0;
                for (size_t i = 0; i < compression.ChunkSizesSize(); ++i) {
                    const auto size = compression.GetChunkSizes(i);
                    const auto chunk = GetCodec(codec)->Decode(
                        TStringBuf(compressed).SubStr(offset, size));
                    UNIT_ASSERT_VALUES_EQUAL(
                        i == 2 ? BlockSize : chunkSize,
                        chunk.size());
                    UNIT_ASSERT_VALUES_EQUAL(
                        TString(chunk.size(), 'a' + i),
                        chunk);
                    offset += size;
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldHandleEmptyPayloadAndReads)
    {
        const TBlobCompressor compressor(
            TabletId,
            BlockSize,
            ChunkSize,
            NProto::TBlobCompression::LZ4);
        const auto [compressed, compression] =
            GetResultOrFail(compressor.CompressBlob({}));

        UNIT_ASSERT(compressed.empty());
        UNIT_ASSERT(NProto::TBlobCompression::LZ4 == compression.GetCodec());
        UNIT_ASSERT_VALUES_EQUAL(ChunkSize, compression.GetChunkSize());
        UNIT_ASSERT_VALUES_EQUAL(0, compression.ChunkSizesSize());
        const auto [emptyQueries, emptyCount, emptyBlockOffsets] =
            GetResultOrFail(compressor.PlanReadBlob({}, {}, {}));
        UNIT_ASSERT(!emptyQueries);
        UNIT_ASSERT_VALUES_EQUAL(0, emptyCount);
        for (const auto& metadata: {compression, MakeCompression()}) {
            const auto [queries, queriesCount, blockOffsetsForEachRequest] =
                GetResultOrFail(compressor.PlanReadBlob(MakeBlobId(metadata), metadata, {}));
            UNIT_ASSERT(!queries);
            UNIT_ASSERT_VALUES_EQUAL(0, queriesCount);
        }
        UNIT_ASSERT(!HasError(
            compressor.DecompressBlob({}, {}, compression, {}, nullptr)));
        UNIT_ASSERT(!HasError(
            compressor
                .DecompressBlob({{}, {}}, {}, MakeCompression(), {}, nullptr)));
    }

    Y_UNIT_TEST(ShouldRoundTripIncompressibleData)
    {
        TFastRng64 rng(123);
        TString payload(3 * ChunkSize, '\0');
        for (auto& byte: payload) {
            byte = static_cast<char>(rng.GenRand());
        }

        const TBlobCompressor compressor(
            TabletId,
            BlockSize,
            ChunkSize,
            NProto::TBlobCompression::LZ4);
        const auto [compressed, compression] =
            GetResultOrFail(compressor.CompressBlob(payload));

        UNIT_ASSERT_VALUES_EQUAL(3, compression.ChunkSizesSize());
        UNIT_ASSERT(compressed.size() > payload.size());
        UNIT_ASSERT_VALUES_EQUAL(
            payload,
            DecompressBlob(
                compressor,
                compression,
                compressed,
                payload.size()));
    }

    Y_UNIT_TEST(ShouldDeduplicateChunksAndCoalesceAdjacentReads)
    {
        const TBlobCompressor compressor(
            TabletId,
            BlockSize,
            ChunkSize,
            NProto::TBlobCompression::LZ4);
        // Sorted, repeated blocks in chunks 0, 1 and 4. Chunks 2 and 3
        // must not be read, even though their bytes lie between the queries.
        const auto [queries, queriesCount, blockOffsetsForEachRequest] =
            GetResultOrFail(compressor.PlanReadBlob(
                MakeBlobId(MakeCompression()),
                MakeCompression(),
                {0, 1, 7, 8, 8, 32, 39}));

        UNIT_ASSERT_VALUES_EQUAL(2, queriesCount);
        for (const auto& query: MakeArrayRef(queries.Get(), queriesCount)) {
            UNIT_ASSERT(query.Id == MakeBlobId(MakeCompression()));
        }
        UNIT_ASSERT_VALUES_EQUAL(0, queries[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(300, queries[0].Size);
        UNIT_ASSERT_VALUES_EQUAL(1000, queries[1].Shift);
        UNIT_ASSERT_VALUES_EQUAL(500, queries[1].Size);
    }

    Y_UNIT_TEST(ShouldPlanSingleAndFullBlobReads)
    {
        const TBlobCompressor compressor(
            TabletId,
            BlockSize,
            ChunkSize,
            NProto::TBlobCompression::LZ4);
        const auto compression = MakeCompression();
        const auto [single, singleCount, singleBlockOffsets] =
            GetResultOrFail(compressor.PlanReadBlob(MakeBlobId(compression), compression, {23}));
        UNIT_ASSERT_VALUES_EQUAL(1, singleCount);
        UNIT_ASSERT_VALUES_EQUAL(300, single[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(300, single[0].Size);

        const auto [full, fullCount, fullBlockOffsets] =
            GetResultOrFail(compressor.PlanReadBlob(
                MakeBlobId(compression),
                compression,
                {0, 8, 16, 24, 32}));
        UNIT_ASSERT_VALUES_EQUAL(1, fullCount);
        UNIT_ASSERT_VALUES_EQUAL(0, full[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(1500, full[0].Size);
    }

    Y_UNIT_TEST(ShouldRejectUnsortedBlockOffsets)
    {
        const TBlobCompressor compressor(
            TabletId,
            BlockSize,
            ChunkSize,
            NProto::TBlobCompression::LZ4);
        const auto blob =
            GetResultOrFail(compressor.CompressBlob(TString(ChunkSize, 'a')));
        const TVector<ui16> blocks = {1, 0};
        TString result(2 * BlockSize, '?');
        const TSgList outputs = {
            {result.begin(), BlockSize},
            {result.begin() + BlockSize, BlockSize}};

        AssertArgumentError(
            [&]
            {
                return compressor.PlanReadBlob(
                                     MakeBlobId(blob.Compression),
                                     blob.Compression,
                                     blocks)
                    .GetError();
            });
        AssertArgumentError(
            [&]
            {
                return compressor.DecompressBlob(
                    {TRope(blob.Data)},
                    outputs,
                    blob.Compression,
                    blocks,
                    nullptr);
            });
    }

    Y_UNIT_TEST(ShouldReadRequestedBlocksFromCompressedPayload)
    {
        for (const ui32 blocksPerChunk: {2, 4, 8}) {
            for (const ui32 blockSize: {512, 4096, 65536, 131072}) {
                const ui32 chunkSize = blocksPerChunk * blockSize;
                const TBlobCompressor compressor(
                    TabletId,
                    blockSize,
                    chunkSize,
                    NProto::TBlobCompression::LZ4);
                TString payload;
                for (ui32 i = 0; i < 32; ++i) {
                    payload.append(TString(blockSize, 'a' + i));
                }
                const auto [compressed, compression] =
                    GetResultOrFail(compressor.CompressBlob(payload));
                const TVector<ui16> blocks = {0, 17, 17, 31};
                const auto [queries, queriesCount, blockOffsetsForEachRequest] =
                    GetResultOrFail(compressor.PlanReadBlob(
                        MakeBlobId(compression),
                        compression,
                        blocks));

                TVector<TRope> inputs;
                for (const auto& query:
                     MakeArrayRef(queries.Get(), queriesCount))
                {
                    inputs.emplace_back(
                        TString(TStringBuf(compressed)
                                    .SubStr(query.Shift, query.Size)));
                }
                TVector<TString> results(
                    blocks.size(),
                    TString(blockSize, '?'));
                TSgList outputs;
                for (auto& result: results) {
                    outputs.push_back({result.begin(), result.size()});
                }
                UNIT_ASSERT(!HasError(compressor.DecompressBlob(
                    inputs,
                    outputs,
                    compression,
                    blocks,
                    nullptr)));
                for (size_t i = 0; i < blocks.size(); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        TString(blockSize, 'a' + blocks[i]),
                        results[i]);
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldRestoreSparseReadsUsingMetadata)
    {
        // The reader uses a different chunk size and, for Zstd blobs, codec.
        // The first requested chunk is not chunk zero.
        const TBlobCompressor reader(
            TabletId,
            BlockSize,
            2 * ChunkSize,
            NProto::TBlobCompression::LZ4);
        for (const auto codec:
             {NProto::TBlobCompression::LZ4, NProto::TBlobCompression::ZSTD})
        {
            const TBlobCompressor writer(TabletId, BlockSize, ChunkSize, codec);
            TString payload;
            for (ui32 i = 0; i < 33; ++i) {
                payload.append(TString(BlockSize, 'a' + i));
            }
            const auto blob = GetResultOrFail(writer.CompressBlob(payload));
            const TVector<ui16> blocks = {8, 9, 17, 17, 32};
            const auto [queries, queriesCount, blockOffsetsForEachRequest] =
                GetResultOrFail(reader.PlanReadBlob(
                    MakeBlobId(blob.Compression),
                    blob.Compression,
                    blocks));
            UNIT_ASSERT_VALUES_EQUAL(2, queriesCount);
            UNIT_ASSERT(queries[0].Shift > 0);
            TVector<TRope> inputs = {{}};
            for (const auto& query: MakeArrayRef(queries.Get(), queriesCount)) {
                inputs.emplace_back(TString(
                    TStringBuf(blob.Data).SubStr(query.Shift, query.Size)));
                inputs.push_back({});
            }

            TVector<TString> results(
                blocks.size(),
                TString(BlockSize + 2, '!'));
            TSgList outputs;
            for (auto& result: results) {
                outputs.push_back({result.begin() + 1, BlockSize});
            }
            UNIT_ASSERT(!HasError(reader.DecompressBlob(
                inputs,
                outputs,
                blob.Compression,
                blocks,
                nullptr)));
            for (size_t i = 0; i < blocks.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL('!', results[i].front());
                UNIT_ASSERT_VALUES_EQUAL('!', results[i].back());
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(BlockSize, 'a' + blocks[i]),
                    TStringBuf(results[i]).SubStr(1, BlockSize));
            }
        }
    }

    Y_UNIT_TEST(ShouldPlanAndDecompressWithDifferentChunkSizes)
    {
        for (const ui32 chunkSize: {2 * BlockSize, 4 * ChunkSize}) {
            const ui32 blocksPerChunk = chunkSize / BlockSize;
            const ui16 lastBlock = 3 * blocksPerChunk;
            TString payload;
            for (ui32 i = 0; i <= lastBlock; ++i) {
                payload.append(TString(BlockSize, static_cast<char>(i)));
            }
            for (
                const auto codec:
                {NProto::TBlobCompression::LZ4, NProto::TBlobCompression::ZSTD})
            {
                const TBlobCompressor writer(
                    TabletId,
                    BlockSize,
                    chunkSize,
                    codec);
                const auto blob = GetResultOrFail(writer.CompressBlob(payload));
                NProto::TBlobCompression metadata;
                UNIT_ASSERT(metadata.ParseFromString(
                    blob.Compression.SerializeAsString()));
                UNIT_ASSERT_VALUES_EQUAL(chunkSize, metadata.GetChunkSize());
                UNIT_ASSERT_VALUES_EQUAL(4, metadata.ChunkSizesSize());

                for (const ui32 readerChunkSize: {4 * BlockSize, 2 * ChunkSize})
                {
                    const TBlobCompressor reader(
                        TabletId,
                        BlockSize,
                        readerChunkSize,
                        NProto::TBlobCompression::LZ4);
                    const TVector<ui16> blocks = {
                        static_cast<ui16>(blocksPerChunk),
                        static_cast<ui16>(blocksPerChunk + 1),
                        lastBlock,
                        lastBlock};
                    const auto
                        [queries, queriesCount, blockOffsetsForEachRequest] =
                            GetResultOrFail(reader.PlanReadBlob(
                                MakeBlobId(metadata),
                                metadata,
                                blocks));
                    UNIT_ASSERT_VALUES_EQUAL(2, queriesCount);
                    UNIT_ASSERT_VALUES_EQUAL(
                        metadata.GetChunkSizes(0),
                        queries[0].Shift);
                    UNIT_ASSERT_VALUES_EQUAL(
                        metadata.GetChunkSizes(1),
                        queries[0].Size);
                    UNIT_ASSERT_VALUES_EQUAL(
                        metadata.GetChunkSizes(0) + metadata.GetChunkSizes(1) +
                            metadata.GetChunkSizes(2),
                        queries[1].Shift);
                    UNIT_ASSERT_VALUES_EQUAL(
                        metadata.GetChunkSizes(3),
                        queries[1].Size);

                    TVector<TRope> inputs;
                    for (const auto& query:
                         MakeArrayRef(queries.Get(), queriesCount))
                    {
                        inputs.emplace_back(
                            TString(TStringBuf(blob.Data).SubStr(
                                query.Shift,
                                query.Size)));
                    }
                    TVector<TString> results(
                        blocks.size(),
                        TString(BlockSize, '?'));
                    TSgList outputs;
                    for (auto& result: results) {
                        outputs.push_back({result.begin(), result.size()});
                    }
                    UNIT_ASSERT(!HasError(reader.DecompressBlob(
                        inputs,
                        outputs,
                        metadata,
                        blocks,
                        nullptr)));
                    for (size_t i = 0; i < blocks.size(); ++i) {
                        UNIT_ASSERT_VALUES_EQUAL(
                            TString(BlockSize, static_cast<char>(blocks[i])),
                            results[i]);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldReadFragmentedRopes)
    {
        for (const auto codec:
             {NProto::TBlobCompression::LZ4, NProto::TBlobCompression::ZSTD})
        {
            const TBlobCompressor compressor(
                TabletId,
                BlockSize,
                ChunkSize,
                codec);
            TString payload;
            for (ui32 i = 0; i < 17; ++i) {
                payload.append(TString(BlockSize, 'a' + i));
            }
            const auto blob = GetResultOrFail(compressor.CompressBlob(payload));
            const TVector<ui16> blocks = {1, 8, 8, 16};
            for (const bool splitEveryChunk: {false, true}) {
                TRope input;
                size_t offset = 0;
                for (const auto size: blob.Compression.GetChunkSizes()) {
                    const auto chunk =
                        TStringBuf(blob.Data).SubStr(offset, size);
                    if (splitEveryChunk || offset == 0) {
                        // Split the length header as well as the payload. Later
                        // chunks are contiguous unless splitEveryChunk is set.
                        input.Insert(
                            input.End(),
                            TRope(TString(chunk.Head(1))));
                        input.Insert(
                            input.End(),
                            TRope(TString(chunk.SubStr(1, 6))));
                        const size_t payloadSplit =
                            sizeof(ui64) + (size - sizeof(ui64)) / 2;
                        input.Insert(
                            input.End(),
                            TRope(TString(chunk.SubStr(7, payloadSplit - 7))));
                        input.Insert(
                            input.End(),
                            TRope(TString(chunk.SubStr(payloadSplit))));
                    } else {
                        input.Insert(input.End(), TRope(TString(chunk)));
                    }
                    offset += size;
                }
                UNIT_ASSERT(!input.IsContiguous());

                TVector<TString> results(
                    blocks.size(),
                    TString(BlockSize, '?'));
                TSgList outputs;
                for (auto& result: results) {
                    outputs.push_back({result.begin(), result.size()});
                }
                UNIT_ASSERT(!HasError(compressor.DecompressBlob(
                    {{}, input, {}},
                    outputs,
                    blob.Compression,
                    blocks,
                    nullptr)));
                for (size_t i = 0; i < blocks.size(); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        TString(BlockSize, 'a' + blocks[i]),
                        results[i]);
                }
                UNIT_ASSERT(!input.IsContiguous());
                UNIT_ASSERT_VALUES_EQUAL(blob.Data, input.ConvertToString());
            }
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidChunkSizeMetadata)
    {
        const TBlobCompressor compressor(
            TabletId,
            BlockSize,
            ChunkSize,
            NProto::TBlobCompression::LZ4);
        const auto blob =
            GetResultOrFail(compressor.CompressBlob(TString(ChunkSize, 'a')));
        TString result(BlockSize, '?');
        const TSgList outputs = {{result.begin(), result.size()}};
        for (const ui32 size: {0u, BlockSize / 2, BlockSize - 1, BlockSize + 1})
        {
            auto metadata = blob.Compression;
            metadata.SetChunkSize(size);
            AssertArgumentError(
                [&]
                {
                    return compressor.PlanReadBlob(
                                         MakeBlobId(metadata),
                                         metadata,
                                         {0})
                        .GetError();
                });
            AssertArgumentError(
                [&]
                {
                    return compressor.DecompressBlob(
                        {TRope(blob.Data)},
                        outputs,
                        metadata,
                        {0},
                        nullptr);
                });
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidDecompressionBuffers)
    {
        const TBlobCompressor compressor(
            TabletId,
            BlockSize,
            ChunkSize,
            NProto::TBlobCompression::LZ4);
        const auto blob =
            GetResultOrFail(compressor.CompressBlob(TString(ChunkSize, 'a')));
        TString result(BlockSize + 1, '?');
        const TSgList outputs = {{result.begin(), BlockSize}};
        const auto data = TStringBuf(blob.Data);

        AssertArgumentError(
            [&]
            {
                return compressor.DecompressBlob(
                    {TRope(TString(data))},
                    {},
                    blob.Compression,
                    {0},
                    nullptr);
            });
        AssertArgumentError(
            [&]
            {
                return compressor.DecompressBlob(
                    {TRope(TString(data))},
                    {{result.begin(), BlockSize - 1}},
                    blob.Compression,
                    {0},
                    nullptr);
            });
        AssertArgumentError(
            [&]
            {
                return compressor.DecompressBlob(
                    {TRope(TString(data))},
                    {{result.begin(), BlockSize + 1}},
                    blob.Compression,
                    {0},
                    nullptr);
            });
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            compressor
                .DecompressBlob({}, outputs, blob.Compression, {0}, nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            compressor
                .DecompressBlob(
                    {TRope(TString(data.Head(1))),
                     TRope(TString(data.SubStr(1)))},
                    outputs,
                    blob.Compression,
                    {0},
                    nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            compressor
                .DecompressBlob(
                    {TRope(TString(data.Head(data.size() - 1)))},
                    outputs,
                    blob.Compression,
                    {0},
                    nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            compressor
                .DecompressBlob(
                    {TRope(TString(data)), TRope(TString("extra"))},
                    outputs,
                    blob.Compression,
                    {0},
                    nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            compressor
                .DecompressBlob(
                    {TRope(TString(data))},
                    {},
                    blob.Compression,
                    {},
                    nullptr)
                .GetCode());
        AssertArgumentError(
            [&]
            {
                return compressor.DecompressBlob(
                    {TRope(TString(data))},
                    outputs,
                    blob.Compression,
                    {8},
                    nullptr);
            });

        // The offset addresses an existing chunk, but lies beyond its partial
        // final payload. Chunk metadata alone cannot detect that in
        // PlanReadBlob.
        const auto partial =
            GetResultOrFail(compressor.CompressBlob(TString(BlockSize, 'a')));
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            compressor
                .DecompressBlob(
                    {TRope(partial.Data)},
                    outputs,
                    partial.Compression,
                    {1},
                    nullptr)
                .GetCode());
    }

    Y_UNIT_TEST(ShouldRejectInvalidCompressedChunksAndMetadata)
    {
        for (const auto codec:
             {NProto::TBlobCompression::LZ4, NProto::TBlobCompression::ZSTD})
        {
            const TBlobCompressor compressor(
                TabletId,
                BlockSize,
                ChunkSize,
                codec);
            const auto blob = GetResultOrFail(
                compressor.CompressBlob(TString(ChunkSize, 'a')));
            TString result(BlockSize, '?');
            const TSgList outputs = {{result.begin(), result.size()}};

            auto metadata = blob.Compression;
            metadata.SetChunkSizes(0, 0);
            AssertArgumentError(
                [&]
                {
                    return compressor.DecompressBlob(
                        {TRope(blob.Data)},
                        outputs,
                        metadata,
                        {0},
                        nullptr);
                });
            metadata = blob.Compression;
            metadata.SetCodec(NProto::TBlobCompression::CODEC_UNSPECIFIED);
            AssertArgumentError(
                [&]
                {
                    return compressor.DecompressBlob(
                        {TRope(blob.Data)},
                        outputs,
                        metadata,
                        {0},
                        nullptr);
                });

            // Reject zero decoded lengths and corrupt compressed payloads.
            TString emptyChunk = blob.Data;
            WriteUnaligned<ui64>(emptyChunk.begin(), 0);
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                compressor
                    .DecompressBlob(
                        {TRope(emptyChunk)},
                        outputs,
                        blob.Compression,
                        {0},
                        nullptr)
                    .GetCode());
            for (
                const ui64 size:
                {ui64(BlockSize - 1), ui64(ChunkSize + BlockSize), Max<ui64>()})
            {
                TString corrupt = blob.Data;
                WriteUnaligned<ui64>(corrupt.begin(), size);
                UNIT_ASSERT_VALUES_EQUAL(
                    E_REJECTED,
                    compressor
                        .DecompressBlob(
                            {TRope(corrupt)},
                            outputs,
                            blob.Compression,
                            {0},
                            nullptr)
                        .GetCode());
            }

            // A valid length header without any compressed payload is corrupt.
            TString header(sizeof(ui64), '\0');
            WriteUnaligned<ui64>(header.begin(), ChunkSize);
            metadata = blob.Compression;
            metadata.SetChunkSizes(0, header.size());
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                compressor
                    .DecompressBlob(
                        {TRope(header)},
                        outputs,
                        metadata,
                        {0},
                        nullptr)
                    .GetCode());

            // A truncated length header must also return a decoding error.
            metadata.SetChunkSizes(0, sizeof(ui64) - 1);
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                compressor
                    .DecompressBlob(
                        {TRope(TString(header.data(), sizeof(ui64) - 1))},
                        outputs,
                        metadata,
                        {0},
                        nullptr)
                    .GetCode());

            // Only the final chunk is allowed to be shorter than ChunkSize.
            const auto partial = GetResultOrFail(
                compressor.CompressBlob(TString(BlockSize, 'a')));
            metadata = partial.Compression;
            metadata.AddChunkSizes(partial.Data.size());
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                compressor
                    .DecompressBlob(
                        {TRope(partial.Data)},
                        outputs,
                        metadata,
                        {0},
                        nullptr)
                    .GetCode());
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidChunkSizes)
    {
        for (const ui32 chunkSize: {BlockSize / 2, BlockSize + 1}) {
            AssertArgumentError(
                [&]
                {
                    return TBlobCompressor(
                               TabletId,
                               BlockSize,
                               chunkSize,
                               NProto::TBlobCompression::LZ4)
                        .CompressBlob({})
                        .GetError();
                });
        }
    }

    Y_UNIT_TEST(ShouldHandleMaximumBlockOffsetAndLargeUncompressedByteOffsets)
    {
        constexpr ui32 blockSize = 128 * 1024;
        constexpr ui16 blockOffset = Max<ui16>();
        constexpr ui32 chunkSize = 2 * blockSize;
        constexpr ui32 compressedChunkSize = 1024;
        NProto::TBlobCompression compression;
        compression.SetChunkSize(chunkSize);
        compression.MutableChunkSizes()->Resize(
            (static_cast<ui32>(blockOffset) + 1) / 2,
            compressedChunkSize);

        const TBlobCompressor compressor(
            TabletId,
            blockSize,
            chunkSize,
            NProto::TBlobCompression::LZ4);
        const auto [queries, queriesCount, blockOffsetsForEachRequest] =
            GetResultOrFail(compressor.PlanReadBlob(
                MakeBlobId(compression),
                compression,
                {blockOffset}));
        UNIT_ASSERT_VALUES_EQUAL(1, queriesCount);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(blockOffset - 1) / 2 * compressedChunkSize,
            queries[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(compressedChunkSize, queries[0].Size);
    }

    Y_UNIT_TEST(ShouldRejectReadsBeyondBlobSize)
    {
        const TBlobCompressor compressor(
            TabletId,
            BlockSize,
            ChunkSize,
            NProto::TBlobCompression::LZ4);
        const auto compression = MakeCompression();
        const NKikimr::TLogoBlobID blobId(TabletId, 1, 2, 3, 99, 4);

        // Chunk zero starts inside the blob but extends past its end; chunk
        // one starts outside the blob. Neither query can be sent to storage.
        for (const ui16 blockOffset: {0, 8}) {
            AssertArgumentError(
                [&]
                {
                    return compressor.PlanReadBlob(
                                         blobId,
                                         compression,
                                         {blockOffset})
                        .GetError();
                });
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidInputs)
    {
        AssertArgumentError(
            [&]
            {
                return TBlobCompressor(
                           TabletId,
                           0,
                           ChunkSize,
                           NProto::TBlobCompression::LZ4)
                    .CompressBlob({})
                    .GetError();
            });
        AssertArgumentError(
            [&]
            {
                return TBlobCompressor(
                           TabletId,
                           BlockSize,
                           0,
                           NProto::TBlobCompression::LZ4)
                    .CompressBlob({})
                    .GetError();
            });
        AssertArgumentError(
            [&]
            {
                return TBlobCompressor(
                           TabletId,
                           BlockSize,
                           ChunkSize,
                           NProto::TBlobCompression::CODEC_UNSPECIFIED)
                    .CompressBlob({})
                    .GetError();
            });

        const TBlobCompressor compressor(
            TabletId,
            BlockSize,
            ChunkSize,
            NProto::TBlobCompression::LZ4);
        AssertArgumentError(
            [&]
            {
                return compressor.CompressBlob("unaligned").GetError();
            });
        AssertArgumentError(
            [&]
            {
                return compressor.PlanReadBlob(
                                     MakeBlobId(MakeCompression()),
                                     MakeCompression(),
                                     {40})
                    .GetError();
            });
        AssertArgumentError(
            [&]
            {
                return compressor.PlanReadBlob({}, {}, {0}).GetError();
            });

        auto compression = MakeCompression();
        compression.SetChunkSizes(2, 0);
        AssertArgumentError(
            [&]
            {
                return compressor.PlanReadBlob(
                                     MakeBlobId(compression),
                                     compression,
                                     {16})
                    .GetError();
            });
    }

    Y_UNIT_TEST(ShouldRejectInvalidReaderSettings)
    {
        const auto compression = MakeCompression();
        const TBlobCompressor reader(TabletId, BlockSize);
        AssertArgumentError(
            [&]
            {
                return reader.CompressBlob({}).GetError();
            });

        const TBlobCompressor invalidReader(TabletId, 0);
        AssertArgumentError(
            [&]
            {
                return invalidReader
                    .PlanReadBlob(MakeBlobId(compression), compression, {0})
                    .GetError();
            });

        TString output(BlockSize, '?');
        TVector<ui32> checksums;
        AssertArgumentError(
            [&]
            {
                return invalidReader.DecompressBlob(
                    {TRope(TString(100, 'a'))},
                    {{output.begin(), output.size()}},
                    compression,
                    {0},
                    &checksums);
            });
        UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), output);
        UNIT_ASSERT(checksums.empty());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
