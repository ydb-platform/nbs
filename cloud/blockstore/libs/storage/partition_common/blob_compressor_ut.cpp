#include "blob_compressor.h"

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/config.h>

#include <library/cpp/blockcodecs/codecs.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/array_ref.h>
#include <util/generic/cast.h>
#include <util/random/fast.h>
#include <util/system/unaligned_mem.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TCompression = NProto::TCompressionMetadata;

constexpr ui64 TabletId = 123456789;
constexpr ui32 BlockSize = 4096;
constexpr ui32 ChunkSize = 32 * 1024;

ui32 GetChunkSize(
    const NProto::TCompressionMetadata& compression,
    ui32 blobSize,
    size_t chunk)
{
    const auto& sizes = compression.GetPlainChunks();
    if (chunk < sizes.CompressedChunkSizeSize()) {
        return sizes.GetCompressedChunkSize(chunk);
    }
    ui32 lastSize = blobSize;
    for (const auto size: sizes.GetCompressedChunkSize()) {
        lastSize -= size;
    }
    return lastSize;
}

TVector<ui32> GetChunkSizes(
    const NProto::TCompressionMetadata& compression,
    ui32 blobSize)
{
    TVector<ui32> sizes;
    for (size_t i = 0;
         i <= compression.GetPlainChunks().CompressedChunkSizeSize();
         ++i)
    {
        sizes.push_back(GetChunkSize(compression, blobSize, i));
    }
    return sizes;
}

NKikimr::TLogoBlobID MakeBlobId(ui32 size = 1500)
{
    return NKikimr::TLogoBlobID(TabletId, 1, 2, 3, size, 4);
}

const NBlockCodecs::ICodec* GetCodec()
{
    return NBlockCodecs::Codec("lz4-fast14-safe");
}

TString DecompressWholeBlob(
    const NProto::TCompressionMetadata& compression,
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
    UNIT_ASSERT(!HasError(DecompressBlob(
        BlockSize,
        {TRope(TString(data))},
        compression,
        data.size(),
        blocks,
        outputs,
        nullptr)));
    return result;
}

template <typename T>
T GetResultOrFail(TResultOrError<T> result)
{
    UNIT_ASSERT_C(!HasError(result.GetError()), FormatError(result.GetError()));
    return result.ExtractResult();
}

TSgList MakeSgList(TStringBuf payload, ui32 blockSize = BlockSize)
{
    if (payload.empty()) {
        return {};
    }
    return GetResultOrFail(SgListNormalize(
        TBlockDataRef(payload.data(), payload.size()),
        blockSize));
}

template <typename TAction>
void AssertArgumentError(const TAction& action)
{
    const auto error = action();
    UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), FormatError(error));
    UNIT_ASSERT(!error.GetMessage().empty());
}

NProto::TCompressionMetadata MakeCompression()
{
    NProto::TCompressionMetadata compression;
    compression.SetCodec(TCompression::COMPRESSION_LZ4_FAST14_SAFE);
    compression.SetChunkSize(ChunkSize);
    for (const ui32 size: {100, 200, 300, 400}) {
        compression.MutablePlainChunks()->AddCompressedChunkSize(size);
    }
    return compression;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlobCompressorTest)
{
    Y_UNIT_TEST(ShouldParseStorageCodecNames)
    {
        struct TCase
        {
            TStringBuf Family;
            TStringBuf CodecName;
            TCompression::ECompressionType Type;
        };

        const TCase cases[] = {
            {"lz4",
             "lz4-fast14-safe",
             TCompression::COMPRESSION_LZ4_FAST14_SAFE},
            {"snappy", "snappy", TCompression::COMPRESSION_SNAPPY},
            {"zlib", "zlib-6", TCompression::COMPRESSION_ZLIB_6},
            {"lzma", "lzma-5", TCompression::COMPRESSION_LZMA_5},
            {"bzip2", "bzip2-6", TCompression::COMPRESSION_BZIP2_6},
            {"fastlz", "fastlz-0", TCompression::COMPRESSION_FASTLZ_0},
            {"zstd", "zstd_3", TCompression::COMPRESSION_ZSTD_3},
            {"brotli",
             "brotli_11",
             TCompression::COMPRESSION_BROTLI_GENERIC_11}};
        const TString payload(BlockSize, 'a');
        for (const auto& test: cases) {
            for (const auto name: {test.Family, test.CodecName}) {
                NProto::TStorageServiceConfig config;
                config.SetBlobCompressionCodec(TString(name));
                const auto codec = GetResultOrFail(ParseBlobCompressionCodec(
                    config.GetBlobCompressionCodec()));
                UNIT_ASSERT(test.Type == codec);
                const auto blob = GetResultOrFail(CompressBlob(
                    BlockSize,
                    ChunkSize,
                    codec,
                    MakeSgList(payload)));
                UNIT_ASSERT(test.Type == blob.Compression.GetCodec());
                UNIT_ASSERT_VALUES_EQUAL(
                    NBlockCodecs::Codec(test.CodecName)->Encode(payload),
                    blob.Data);
            }
        }
    }

    Y_UNIT_TEST(ShouldRejectUnsupportedStorageCodecNames)
    {
        for (const TStringBuf name:
             {"", "unknown-codec", "null", "lz4-unknown", "zstd06_3"})
        {
            AssertArgumentError(
                [&] { return ParseBlobCompressionCodec(name).GetError(); });
        }
    }

    Y_UNIT_TEST(ShouldSkipMetricsWhenNotConfigured)
    {
        const TString payload(BlockSize, 'a');
        const TVector<std::optional<TBlobCompressor::TMetricsConfig>> configs =
            {std::nullopt, TBlobCompressor::TMetricsConfig{}};
        for (const auto& config: configs) {
            const TBlobCompressor compressor(
                TCompression::COMPRESSION_LZ4_FAST14_SAFE,
                config);
            compressor.CalculateCompressionMetrics({}, payload);
        }
    }

    Y_UNIT_TEST(ShouldCalculateCompressionMetrics)
    {
        const TVector<ui64> chunkSizes = {2 * BlockSize, BlockSize, BlockSize};
        NProto::TDiagnosticsConfig proto;
        for (const auto size: chunkSizes) {
            proto.AddBlobCompressionChunkSizes(size);
        }
        const TDiagnosticsConfig diagnostics(proto);
        const TString payload(2 * BlockSize, 'a');
        auto counters = std::make_shared<TThreadSafePartCounters>(
            std::make_unique<TPartitionDiskCounters>(
                EPublishingPolicy::Repl,
                diagnostics));
        const TBlobCompressor compressor(
            TCompression::COMPRESSION_LZ4_FAST14_SAFE,
            TBlobCompressor::TMetricsConfig{
                .CompressionRate = 1,
                .ChunkSizes = chunkSizes,
                .PartCounters = counters});

        compressor.CalculateCompressionMetrics({}, payload);
        counters->Access(
            [&](const auto& values)
            {
                UNIT_ASSERT_VALUES_EQUAL(
                    payload.size(),
                    values->Cumulative.UncompressedBytesWritten.Value);
                UNIT_ASSERT(
                    values->Cumulative.CompressedBytesWritten.Value > 0);
                UNIT_ASSERT(
                    values->Cumulative.CompressedBytesWritten.Value <
                    payload.size());
                UNIT_ASSERT_VALUES_EQUAL(2, values->BlobCompression.size());
                for (const auto& [size, compression]: values->BlobCompression) {
                    // Repeated chunk sizes must be counted only once.
                    UNIT_ASSERT_VALUES_EQUAL(
                        payload.size(),
                        compression.UncompressedBytesWritten.Value);
                    UNIT_ASSERT(compression.CompressedBytesWritten.Value > 0);
                    UNIT_ASSERT(
                        compression.CompressedBytesWritten.Value <
                        payload.size());
                }
            });
    }

    Y_UNIT_TEST(ShouldReturnCompressedChunksAndMetadata)
    {
        for (const ui32 chunkSize: {16 * 1024, 64 * 1024}) {
            // Two complete chunks followed by one block.
            const TString payload = TString(chunkSize, 'a') +
                                    TString(chunkSize, 'b') +
                                    TString(BlockSize, 'c');
            const auto [compressed, compression] = GetResultOrFail(CompressBlob(
                BlockSize,
                chunkSize,
                TCompression::COMPRESSION_LZ4_FAST14_SAFE,
                MakeSgList(payload)));

            UNIT_ASSERT(
                TCompression::COMPRESSION_LZ4_FAST14_SAFE ==
                compression.GetCodec());
            UNIT_ASSERT_VALUES_EQUAL(chunkSize, compression.GetChunkSize());
            UNIT_ASSERT(compression.HasPlainChunks());
            UNIT_ASSERT_VALUES_EQUAL(
                2,
                compression.GetPlainChunks().CompressedChunkSizeSize());
            UNIT_ASSERT(compressed.size() < payload.size());
            UNIT_ASSERT_VALUES_EQUAL(
                payload,
                DecompressWholeBlob(compression, compressed, payload.size()));

            ui64 offset = 0;
            for (size_t i = 0;
                 i <= compression.GetPlainChunks().CompressedChunkSizeSize();
                 ++i)
            {
                const auto size =
                    GetChunkSize(compression, compressed.size(), i);
                const auto chunk = GetCodec()->Decode(
                    TStringBuf(compressed).SubStr(offset, size));
                UNIT_ASSERT_VALUES_EQUAL(
                    i == 2 ? BlockSize : chunkSize,
                    chunk.size());
                UNIT_ASSERT_VALUES_EQUAL(TString(chunk.size(), 'a' + i), chunk);
                offset += size;
            }
        }
    }

    Y_UNIT_TEST(ShouldReadUsingSerializedCodec)
    {
        const TString payload =
            TString(ChunkSize, 'a') + TString(BlockSize, 'b');
        for (const TStringBuf codecName:
             {"lz4-fast14-safe",
              "snappy",
              "zlib-6",
              "lzma-5",
              "bzip2-6",
              "fastlz-0",
              "zstd_3",
              "brotli_11"})
        {
            const auto codec =
                GetResultOrFail(ParseBlobCompressionCodec(codecName));
            const auto blob = GetResultOrFail(
                CompressBlob(BlockSize, ChunkSize, codec, MakeSgList(payload)));
            NProto::TCompressionMetadata metadata;
            UNIT_ASSERT(
                metadata.ParseFromString(blob.Compression.SerializeAsString()));
            UNIT_ASSERT(codec == metadata.GetCodec());

            UNIT_ASSERT_VALUES_EQUAL(
                payload,
                DecompressWholeBlob(metadata, blob.Data, payload.size()));
        }
    }

    Y_UNIT_TEST(ShouldPlanAndDecompressWithoutConfiguredCodec)
    {
        const TString payload = TString(ChunkSize, 'a') +
                                TString(ChunkSize, 'b') +
                                TString(BlockSize, 'c');
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            TCompression::COMPRESSION_LZ4_FAST14_SAFE,
            MakeSgList(payload)));
        const TVector<ui16> blocks = {1, 2, 16};

        const auto plan = GetResultOrFail(PlanReadBlob(
            BlockSize,
            MakeBlobId(blob.Data.size()),
            blob.Compression,
            blocks));
        UNIT_ASSERT_VALUES_EQUAL(2, plan.QueriesCount);
        UNIT_ASSERT_VALUES_EQUAL(0, plan.Queries[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(
            GetChunkSize(blob.Compression, blob.Data.size(), 0),
            plan.Queries[0].Size);
        UNIT_ASSERT_VALUES_EQUAL(
            GetChunkSize(blob.Compression, blob.Data.size(), 0) +
                GetChunkSize(blob.Compression, blob.Data.size(), 1),
            plan.Queries[1].Shift);
        UNIT_ASSERT_VALUES_EQUAL(
            GetChunkSize(blob.Compression, blob.Data.size(), 2),
            plan.Queries[1].Size);

        TVector<TRope> inputs;
        for (const auto& query:
             MakeArrayRef(plan.Queries.Get(), plan.QueriesCount))
        {
            inputs.emplace_back(
                TString(TStringBuf(blob.Data).SubStr(query.Shift, query.Size)));
        }
        TString result(blocks.size() * BlockSize, '?');
        const auto error = DecompressBlob(
            BlockSize,
            inputs,
            blob.Compression,
            blob.Data.size(),
            blocks,
            MakeSgList(result),
            nullptr);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        for (size_t i = 0; i < blocks.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                TStringBuf(payload).SubStr(blocks[i] * BlockSize, BlockSize),
                TStringBuf(result).SubStr(i * BlockSize, BlockSize));
        }
    }

    Y_UNIT_TEST(ShouldCompressSeparateBlockBuffers)
    {
        for (const size_t blockCount: {1, 8, 9, 17}) {
            TVector<TString> blocks;
            for (size_t i = 0; i < blockCount; ++i) {
                blocks.emplace_back(BlockSize, 'a' + i);
            }
            TSgList input;
            TString payload;
            for (size_t i = 0; i < blockCount; ++i) {
                if (i % 3 == 0) {
                    input.push_back(TBlockDataRef::CreateZeroBlock(BlockSize));
                    payload.append(TString(BlockSize, '\0'));
                } else {
                    input.emplace_back(blocks[i].data(), BlockSize);
                    payload.append(blocks[i]);
                }
            }

            const auto blob = GetResultOrFail(CompressBlob(
                BlockSize,
                ChunkSize,
                TCompression::COMPRESSION_LZ4_FAST14_SAFE,
                input));
            UNIT_ASSERT_VALUES_EQUAL(
                (blockCount + ChunkSize / BlockSize - 1) /
                        (ChunkSize / BlockSize) -
                    1,
                blob.Compression.GetPlainChunks().CompressedChunkSizeSize());
            UNIT_ASSERT_VALUES_EQUAL(
                payload,
                DecompressWholeBlob(
                    blob.Compression,
                    blob.Data,
                    payload.size()));
        }
    }

    Y_UNIT_TEST(ShouldRequireOneBufferPerBlock)
    {
        const TString payload(2 * BlockSize, 'a');
        const TVector<TSgList> inputs = {
            {{}},
            {{payload.data(), BlockSize / 2}},
            {{payload.data(), 2 * BlockSize}},
            {{payload.data(), BlockSize / 2},
             {payload.data() + BlockSize / 2, BlockSize / 2}},
            {{payload.data(), BlockSize},
             {payload.data() + BlockSize, BlockSize - 1}}};
        for (const auto& input: inputs) {
            AssertArgumentError(
                [&]
                {
                    return CompressBlob(
                               BlockSize,
                               ChunkSize,
                               TCompression::COMPRESSION_LZ4_FAST14_SAFE,
                               input)
                        .GetError();
                });
        }
    }

    Y_UNIT_TEST(ShouldHandleEmptyPayloadAndReads)
    {
        const auto [compressed, compression] = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            TCompression::COMPRESSION_LZ4_FAST14_SAFE,
            {}));

        UNIT_ASSERT(compressed.empty());
        UNIT_ASSERT(
            TCompression::COMPRESSION_LZ4_FAST14_SAFE ==
            compression.GetCodec());
        UNIT_ASSERT_VALUES_EQUAL(ChunkSize, compression.GetChunkSize());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            compression.GetPlainChunks().CompressedChunkSizeSize());
        const auto [emptyQueries, emptyCount, emptyBlockOffsets] =
            GetResultOrFail(PlanReadBlob(BlockSize, {}, {}, {}));
        UNIT_ASSERT(!emptyQueries);
        UNIT_ASSERT_VALUES_EQUAL(0, emptyCount);
        for (const auto& metadata: {compression, MakeCompression()}) {
            const auto [queries, queriesCount, blockOffsetsForEachRequest] =
                GetResultOrFail(
                    PlanReadBlob(BlockSize, MakeBlobId(), metadata, {}));
            UNIT_ASSERT(!queries);
            UNIT_ASSERT_VALUES_EQUAL(0, queriesCount);
        }
        UNIT_ASSERT(!HasError(
            DecompressBlob(BlockSize, {}, compression, 0, {}, {}, nullptr)));
        UNIT_ASSERT(!HasError(DecompressBlob(
            BlockSize,
            {{}, {}},
            MakeCompression(),
            1500,
            {},
            {},
            nullptr)));
    }

    Y_UNIT_TEST(ShouldRoundTripIncompressibleData)
    {
        TFastRng64 rng(123);
        TString payload(3 * ChunkSize, '\0');
        for (auto& byte: payload) {
            byte = static_cast<char>(rng.GenRand());
        }

        const auto [compressed, compression] = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            TCompression::COMPRESSION_LZ4_FAST14_SAFE,
            MakeSgList(payload)));

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            compression.GetPlainChunks().CompressedChunkSizeSize());
        UNIT_ASSERT(compressed.size() > payload.size());
        UNIT_ASSERT_VALUES_EQUAL(
            payload,
            DecompressWholeBlob(compression, compressed, payload.size()));
    }

    Y_UNIT_TEST(ShouldDeduplicateChunksAndCoalesceAdjacentReads)
    {
        // Sorted, distinct blocks in chunks 0, 1 and 4. Chunks 2 and 3
        // must not be read, even though their bytes lie between the queries.
        const auto [queries, queriesCount, blockOffsetsForEachRequest] =
            GetResultOrFail(PlanReadBlob(
                BlockSize,
                MakeBlobId(),
                MakeCompression(),
                {0, 1, 7, 8, 9, 32, 39}));

        UNIT_ASSERT_VALUES_EQUAL(2, queriesCount);
        for (const auto& query: MakeArrayRef(queries.Get(), queriesCount)) {
            UNIT_ASSERT(query.Id == MakeBlobId());
        }
        UNIT_ASSERT_VALUES_EQUAL(0, queries[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(300, queries[0].Size);
        UNIT_ASSERT_VALUES_EQUAL(1000, queries[1].Shift);
        UNIT_ASSERT_VALUES_EQUAL(500, queries[1].Size);
        UNIT_ASSERT_VALUES_EQUAL(2, blockOffsetsForEachRequest.size());
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui32>{0, 1, 7, 8, 9}),
            blockOffsetsForEachRequest[0]);
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui32>{32, 39}),
            blockOffsetsForEachRequest[1]);
    }

    Y_UNIT_TEST(ShouldPlanSingleAndFullBlobReads)
    {
        const auto compression = MakeCompression();
        const auto [single, singleCount, singleBlockOffsets] = GetResultOrFail(
            PlanReadBlob(BlockSize, MakeBlobId(), compression, {23}));
        UNIT_ASSERT_VALUES_EQUAL(1, singleCount);
        UNIT_ASSERT_VALUES_EQUAL(300, single[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(300, single[0].Size);

        const auto [full, fullCount, fullBlockOffsets] =
            GetResultOrFail(PlanReadBlob(
                BlockSize,
                MakeBlobId(),
                compression,
                {0, 8, 16, 24, 32}));
        UNIT_ASSERT_VALUES_EQUAL(1, fullCount);
        UNIT_ASSERT_VALUES_EQUAL(0, full[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(1500, full[0].Size);
    }

    Y_UNIT_TEST(ShouldRejectUnsortedBlockOffsets)
    {
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            TCompression::COMPRESSION_LZ4_FAST14_SAFE,
            MakeSgList(TString(ChunkSize, 'a'))));
        const TVector<ui16> blocks = {1, 0};
        TString result(2 * BlockSize, '?');
        const TSgList outputs = {
            {result.begin(), BlockSize},
            {result.begin() + BlockSize, BlockSize}};

        AssertArgumentError(
            [&]
            {
                return PlanReadBlob(
                           BlockSize,
                           MakeBlobId(blob.Data.size()),
                           blob.Compression,
                           blocks)
                    .GetError();
            });
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    {TRope(blob.Data)},
                    blob.Compression,
                    blob.Data.size(),
                    blocks,
                    outputs,
                    nullptr);
            });
    }

    Y_UNIT_TEST(ShouldReadRequestedBlocksFromCompressedPayload)
    {
        for (const ui32 blocksPerChunk: {2, 4, 8}) {
            for (const ui32 blockSize: {512, 4096, 65536, 131072}) {
                const ui32 chunkSize = blocksPerChunk * blockSize;
                TString payload;
                for (ui32 i = 0; i < 32; ++i) {
                    payload.append(TString(blockSize, 'a' + i));
                }
                const auto [compressed, compression] =
                    GetResultOrFail(CompressBlob(
                        blockSize,
                        chunkSize,
                        TCompression::COMPRESSION_LZ4_FAST14_SAFE,
                        MakeSgList(payload, blockSize)));
                const TVector<ui16> blocks = {0, 17, 18, 31};
                const auto [queries, queriesCount, blockOffsetsForEachRequest] =
                    GetResultOrFail(PlanReadBlob(
                        blockSize,
                        MakeBlobId(compressed.size()),
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
                UNIT_ASSERT(!HasError(DecompressBlob(
                    blockSize,
                    inputs,
                    compression,
                    compressed.size(),
                    blocks,
                    outputs,
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
        // The first requested chunk is not chunk zero.
        TString payload;
        for (ui32 i = 0; i < 33; ++i) {
            payload.append(TString(BlockSize, 'a' + i));
        }
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            TCompression::COMPRESSION_LZ4_FAST14_SAFE,
            MakeSgList(payload)));
        const TVector<ui16> blocks = {8, 9, 17, 18, 32};
        const auto [queries, queriesCount, blockOffsetsForEachRequest] =
            GetResultOrFail(PlanReadBlob(
                BlockSize,
                MakeBlobId(blob.Data.size()),
                blob.Compression,
                blocks));
        UNIT_ASSERT_VALUES_EQUAL(2, queriesCount);
        UNIT_ASSERT(queries[0].Shift > 0);
        TVector<TRope> inputs = {{}};
        for (const auto& query: MakeArrayRef(queries.Get(), queriesCount)) {
            inputs.emplace_back(
                TString(TStringBuf(blob.Data).SubStr(query.Shift, query.Size)));
            inputs.push_back({});
        }

        TVector<TString> results(blocks.size(), TString(BlockSize + 2, '!'));
        TSgList outputs;
        for (auto& result: results) {
            outputs.push_back({result.begin() + 1, BlockSize});
        }
        UNIT_ASSERT(!HasError(DecompressBlob(
            BlockSize,
            inputs,
            blob.Compression,
            blob.Data.size(),
            blocks,
            outputs,
            nullptr)));
        for (size_t i = 0; i < blocks.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL('!', results[i].front());
            UNIT_ASSERT_VALUES_EQUAL('!', results[i].back());
            UNIT_ASSERT_VALUES_EQUAL(
                TString(BlockSize, 'a' + blocks[i]),
                TStringBuf(results[i]).SubStr(1, BlockSize));
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
            const auto blob = GetResultOrFail(CompressBlob(
                BlockSize,
                chunkSize,
                TCompression::COMPRESSION_LZ4_FAST14_SAFE,
                MakeSgList(payload)));
            NProto::TCompressionMetadata metadata;
            UNIT_ASSERT(
                metadata.ParseFromString(blob.Compression.SerializeAsString()));
            UNIT_ASSERT_VALUES_EQUAL(chunkSize, metadata.GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                3,
                metadata.GetPlainChunks().CompressedChunkSizeSize());

            const TVector<ui16> blocks = {
                static_cast<ui16>(blocksPerChunk),
                static_cast<ui16>(blocksPerChunk + 1),
                lastBlock};
            const auto [queries, queriesCount, blockOffsetsForEachRequest] =
                GetResultOrFail(PlanReadBlob(
                    BlockSize,
                    MakeBlobId(blob.Data.size()),
                    metadata,
                    blocks));
            UNIT_ASSERT_VALUES_EQUAL(2, queriesCount);
            UNIT_ASSERT_VALUES_EQUAL(
                GetChunkSize(metadata, blob.Data.size(), 0),
                queries[0].Shift);
            UNIT_ASSERT_VALUES_EQUAL(
                GetChunkSize(metadata, blob.Data.size(), 1),
                queries[0].Size);
            UNIT_ASSERT_VALUES_EQUAL(
                GetChunkSize(metadata, blob.Data.size(), 0) +
                    GetChunkSize(metadata, blob.Data.size(), 1) +
                    GetChunkSize(metadata, blob.Data.size(), 2),
                queries[1].Shift);
            UNIT_ASSERT_VALUES_EQUAL(
                GetChunkSize(metadata, blob.Data.size(), 3),
                queries[1].Size);

            TVector<TRope> inputs;
            for (const auto& query: MakeArrayRef(queries.Get(), queriesCount)) {
                inputs.emplace_back(TString(
                    TStringBuf(blob.Data).SubStr(query.Shift, query.Size)));
            }
            TVector<TString> results(blocks.size(), TString(BlockSize, '?'));
            TSgList outputs;
            for (auto& result: results) {
                outputs.push_back({result.begin(), result.size()});
            }
            UNIT_ASSERT(!HasError(DecompressBlob(
                BlockSize,
                inputs,
                metadata,
                blob.Data.size(),
                blocks,
                outputs,
                nullptr)));
            for (size_t i = 0; i < blocks.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(BlockSize, static_cast<char>(blocks[i])),
                    results[i]);
            }
        }
    }

    Y_UNIT_TEST(ShouldReadFragmentedRopes)
    {
        TString payload;
        for (ui32 i = 0; i < 17; ++i) {
            payload.append(TString(BlockSize, 'a' + i));
        }
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            TCompression::COMPRESSION_LZ4_FAST14_SAFE,
            MakeSgList(payload)));
        const TVector<ui16> blocks = {1, 8, 9, 16};
        for (const bool splitEveryChunk: {false, true}) {
            TRope input;
            size_t offset = 0;
            for (const auto size:
                 GetChunkSizes(blob.Compression, blob.Data.size()))
            {
                const auto chunk = TStringBuf(blob.Data).SubStr(offset, size);
                if (splitEveryChunk || offset == 0) {
                    // Split the length header as well as the payload. Later
                    // chunks are contiguous unless splitEveryChunk is set.
                    input.Insert(input.End(), TRope(TString(chunk.Head(1))));
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

            TVector<TString> results(blocks.size(), TString(BlockSize, '?'));
            TSgList outputs;
            for (auto& result: results) {
                outputs.push_back({result.begin(), result.size()});
            }
            const size_t firstChunkSize =
                GetChunkSize(blob.Compression, blob.Data.size(), 0);
            const TVector<size_t> splits = {
                0,
                1,
                7,
                firstChunkSize - 1,
                firstChunkSize,
                firstChunkSize + 1,
                input.GetSize() - 1,
                input.GetSize()};
            for (const auto split: splits) {
                const TVector<TRope> inputs = {
                    {},
                    TRope(input.Begin(), input.Position(split)),
                    {},
                    TRope(input.Position(split), input.End()),
                    {}};
                for (auto& result: results) {
                    std::fill_n(result.begin(), result.size(), '?');
                }
                const auto error = DecompressBlob(
                    BlockSize,
                    inputs,
                    blob.Compression,
                    blob.Data.size(),
                    blocks,
                    outputs,
                    nullptr);
                UNIT_ASSERT_C(!HasError(error), FormatError(error));
                for (size_t i = 0; i < blocks.size(); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        TString(BlockSize, 'a' + blocks[i]),
                        results[i]);
                }
            }
            UNIT_ASSERT(!input.IsContiguous());
            UNIT_ASSERT_VALUES_EQUAL(blob.Data, input.ConvertToString());
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidChunkSizeMetadata)
    {
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            TCompression::COMPRESSION_LZ4_FAST14_SAFE,
            MakeSgList(TString(ChunkSize, 'a'))));
        TString result(BlockSize, '?');
        const TSgList outputs = {{result.begin(), result.size()}};
        for (const ui32 size: {0u, BlockSize / 2, BlockSize - 1, BlockSize + 1})
        {
            auto metadata = blob.Compression;
            metadata.SetChunkSize(size);
            AssertArgumentError(
                [&]
                {
                    return PlanReadBlob(
                               BlockSize,
                               MakeBlobId(blob.Data.size()),
                               metadata,
                               {0})
                        .GetError();
                });
            AssertArgumentError(
                [&]
                {
                    return DecompressBlob(
                        BlockSize,
                        {TRope(blob.Data)},
                        metadata,
                        blob.Data.size(),
                        {0},
                        outputs,
                        nullptr);
                });
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidDecompressionBuffers)
    {
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            TCompression::COMPRESSION_LZ4_FAST14_SAFE,
            MakeSgList(TString(ChunkSize, 'a'))));
        TString result(BlockSize + 1, '?');
        const TSgList outputs = {{result.begin(), BlockSize}};
        const auto data = TStringBuf(blob.Data);

        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    {TRope(TString(data))},
                    blob.Compression,
                    blob.Data.size(),
                    {0},
                    {},
                    nullptr);
            });
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    {TRope(TString(data))},
                    blob.Compression,
                    blob.Data.size(),
                    {0},
                    {{result.begin(), BlockSize - 1}},
                    nullptr);
            });
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    {TRope(TString(data))},
                    blob.Compression,
                    blob.Data.size(),
                    {0},
                    {{result.begin(), BlockSize + 1}},
                    nullptr);
            });
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                {},
                blob.Compression,
                blob.Data.size(),
                {0},
                outputs,
                nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                {{}, {}},
                blob.Compression,
                blob.Data.size(),
                {0},
                outputs,
                nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                {TRope(TString(data.Head(data.size() - 1)))},
                blob.Compression,
                blob.Data.size(),
                {0},
                outputs,
                nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                {TRope(TString(data)), TRope(TString("extra"))},
                blob.Compression,
                blob.Data.size(),
                {0},
                outputs,
                nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                {TRope(TString(data))},
                blob.Compression,
                blob.Data.size(),
                {},
                {},
                nullptr)
                .GetCode());
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    {TRope(TString(data))},
                    blob.Compression,
                    blob.Data.size(),
                    {8},
                    outputs,
                    nullptr);
            });

        // The offset addresses an existing chunk, but lies beyond its partial
        // final payload. Chunk metadata alone cannot detect that in
        // PlanReadBlob.
        const auto partial = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            TCompression::COMPRESSION_LZ4_FAST14_SAFE,
            MakeSgList(TString(BlockSize, 'a'))));
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                {TRope(partial.Data)},
                partial.Compression,
                partial.Data.size(),
                {1},
                outputs,
                nullptr)
                .GetCode());
    }

    Y_UNIT_TEST(ShouldRejectTruncatedChunksAcrossRopes)
    {
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            TCompression::COMPRESSION_LZ4_FAST14_SAFE,
            MakeSgList(TString(ChunkSize, 'a') + TString(BlockSize, 'b'))));
        const auto data = TStringBuf(blob.Data);
        const size_t firstChunkSize =
            GetChunkSize(blob.Compression, blob.Data.size(), 0);
        const TVector<size_t> sizes = {
            firstChunkSize - 1,
            firstChunkSize,
            data.size() - 1};
        TString result(2 * BlockSize, '?');
        for (const auto size: sizes) {
            const auto error = DecompressBlob(
                BlockSize,
                {{},
                 TRope(TString(data.Head(1))),
                 {},
                 TRope(TString(data.SubStr(1, size - 1))),
                 {}},
                blob.Compression,
                blob.Data.size(),
                {0, ChunkSize / BlockSize},
                MakeSgList(result),
                nullptr);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT(
                error.GetMessage().Contains("Incomplete compressed chunk"));
        }
    }

    Y_UNIT_TEST(ShouldRejectChunkWithInflatedDecodedLength)
    {
        auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            TCompression::COMPRESSION_LZ4_FAST14_SAFE,
            MakeSgList(TString(BlockSize, 'a'))));
        // The header claims a full chunk, but LZ4 produces only one block.
        WriteUnaligned<ui64>(blob.Data.begin(), ChunkSize);

        TString result(BlockSize, '?');
        const auto error = DecompressBlob(
            BlockSize,
            {TRope(blob.Data)},
            blob.Compression,
            blob.Data.size(),
            {2},
            {{result.begin(), result.size()}},
            nullptr);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            error.GetCode(),
            FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), result);
    }

    Y_UNIT_TEST(ShouldRejectInvalidCompressedChunksAndMetadata)
    {
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            TCompression::COMPRESSION_LZ4_FAST14_SAFE,
            MakeSgList(TString(ChunkSize, 'a'))));
        TString result(BlockSize, '?');
        const TSgList outputs = {{result.begin(), result.size()}};

        auto metadata = blob.Compression;
        metadata.MutablePlainChunks()->AddCompressedChunkSize(0);
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    {TRope(blob.Data)},
                    metadata,
                    blob.Data.size(),
                    {0},
                    outputs,
                    nullptr);
            });
        for (const int value: {-1, Max<int>()}) {
            const auto codec =
                static_cast<TCompression::ECompressionType>(value);
            metadata = blob.Compression;
            metadata.SetCodec(codec);
            AssertArgumentError(
                [&]
                {
                    return DecompressBlob(
                        BlockSize,
                        {TRope(blob.Data)},
                        metadata,
                        blob.Data.size(),
                        {0},
                        outputs,
                        nullptr);
                });
        }

        // Reject zero decoded lengths and corrupt compressed payloads.
        TString emptyChunk = blob.Data;
        WriteUnaligned<ui64>(emptyChunk.begin(), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                {TRope(emptyChunk)},
                blob.Compression,
                blob.Data.size(),
                {0},
                outputs,
                nullptr)
                .GetCode());
        for (const ui64 size:
             {ui64(BlockSize - 1), ui64(ChunkSize + BlockSize), Max<ui64>()})
        {
            TString corrupt = blob.Data;
            WriteUnaligned<ui64>(corrupt.begin(), size);
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                DecompressBlob(
                    BlockSize,
                    {TRope(corrupt)},
                    blob.Compression,
                    blob.Data.size(),
                    {0},
                    outputs,
                    nullptr)
                    .GetCode());
        }

        // A valid length header without any compressed payload is corrupt.
        TString header(sizeof(ui64), '\0');
        WriteUnaligned<ui64>(header.begin(), ChunkSize);
        metadata = blob.Compression;
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                {TRope(header)},
                metadata,
                header.size(),
                {0},
                outputs,
                nullptr)
                .GetCode());

        // A truncated length header must also return a decoding error.
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                {TRope(TString(header.data(), sizeof(ui64) - 1))},
                metadata,
                sizeof(ui64) - 1,
                {0},
                outputs,
                nullptr)
                .GetCode());

        // Only the final chunk is allowed to be shorter than ChunkSize.
        const auto partial = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            TCompression::COMPRESSION_LZ4_FAST14_SAFE,
            MakeSgList(TString(BlockSize, 'a'))));
        metadata = partial.Compression;
        metadata.MutablePlainChunks()->AddCompressedChunkSize(
            partial.Data.size());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                {TRope(partial.Data)},
                metadata,
                2 * partial.Data.size(),
                {0},
                outputs,
                nullptr)
                .GetCode());
    }

    Y_UNIT_TEST(ShouldRejectInvalidChunkSizes)
    {
        for (const ui32 chunkSize: {BlockSize / 2, BlockSize + 1}) {
            AssertArgumentError(
                [&]
                {
                    return CompressBlob(
                               BlockSize,
                               chunkSize,
                               TCompression::COMPRESSION_LZ4_FAST14_SAFE,
                               {})
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
        NProto::TCompressionMetadata compression;
        compression.SetChunkSize(chunkSize);
        for (ui32 i = 1; i < (static_cast<ui32>(blockOffset) + 1) / 2; ++i) {
            compression.MutablePlainChunks()->AddCompressedChunkSize(
                compressedChunkSize);
        }

        const auto [queries, queriesCount, blockOffsetsForEachRequest] =
            GetResultOrFail(PlanReadBlob(
                blockSize,
                MakeBlobId(
                    (static_cast<ui32>(blockOffset) + 1) / 2 *
                    compressedChunkSize),
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
        const auto compression = MakeCompression();
        const NKikimr::TLogoBlobID blobId(TabletId, 1, 2, 3, 99, 4);

        // Chunk zero starts inside the blob but extends past its end; chunk
        // one starts outside the blob. Neither query can be sent to storage.
        for (const ui16 blockOffset: {0, 8}) {
            AssertArgumentError(
                [&]
                {
                    return PlanReadBlob(
                               BlockSize,
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
                return CompressBlob(
                           0,
                           ChunkSize,
                           TCompression::COMPRESSION_LZ4_FAST14_SAFE,
                           {})
                    .GetError();
            });
        AssertArgumentError(
            [&]
            {
                return CompressBlob(
                           BlockSize,
                           0,
                           TCompression::COMPRESSION_LZ4_FAST14_SAFE,
                           {})
                    .GetError();
            });

        AssertArgumentError(
            [&]
            {
                return CompressBlob(
                           BlockSize,
                           ChunkSize,
                           TCompression::COMPRESSION_LZ4_FAST14_SAFE,
                           {{"unaligned", 9}})
                    .GetError();
            });
        AssertArgumentError(
            [&]
            {
                return PlanReadBlob(
                           BlockSize,
                           MakeBlobId(),
                           MakeCompression(),
                           {40})
                    .GetError();
            });
        AssertArgumentError(
            [&] { return PlanReadBlob(BlockSize, {}, {}, {0}).GetError(); });

        auto compression = MakeCompression();
        compression.MutablePlainChunks()->SetCompressedChunkSize(2, 0);
        AssertArgumentError(
            [&]
            {
                return PlanReadBlob(BlockSize, MakeBlobId(), compression, {16})
                    .GetError();
            });
    }

    Y_UNIT_TEST(ShouldReadSingleChunkWithoutStoredSizesAndCalculateChecksums)
    {
        const TString payload =
            TString(BlockSize, 'a') + TString(BlockSize, 'b');
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            TCompression::COMPRESSION_LZ4_FAST14_SAFE,
            MakeSgList(payload)));
        UNIT_ASSERT(blob.Compression.HasPlainChunks());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            blob.Compression.GetPlainChunks().CompressedChunkSizeSize());

        const TVector<ui16> blocks = {0, 1};
        const auto plan = GetResultOrFail(PlanReadBlob(
            BlockSize,
            MakeBlobId(blob.Data.size()),
            blob.Compression,
            blocks));
        UNIT_ASSERT_VALUES_EQUAL(1, plan.QueriesCount);
        UNIT_ASSERT_VALUES_EQUAL(0, plan.Queries[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(blob.Data.size(), plan.Queries[0].Size);

        TString result(blocks.size() * BlockSize, '?');
        TVector<ui32> checksums;
        const auto error = DecompressBlob(
            BlockSize,
            {TRope(blob.Data)},
            blob.Compression,
            blob.Data.size(),
            blocks,
            MakeSgList(result),
            &checksums);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        UNIT_ASSERT_VALUES_EQUAL(payload, result);
        UNIT_ASSERT_VALUES_EQUAL(blocks.size(), checksums.size());
        for (size_t i = 0; i < blocks.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                ComputeDefaultDigest(
                    {payload.data() + blocks[i] * BlockSize, BlockSize}),
                checksums[i]);
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidCompressedChunkSizes)
    {
        TString output(BlockSize, '?');
        const TVector<TVector<ui32>> invalidSizes = {
            {0},
            {100, 0},
            {1500},
            {1501},
            {100, 1400},
            {100, 1401},
            {Max<ui32>()},
            {1, Max<ui32>()}};
        for (const auto& sizes: invalidSizes) {
            TCompression metadata;
            metadata.SetChunkSize(ChunkSize);
            for (const auto size: sizes) {
                metadata.MutablePlainChunks()->AddCompressedChunkSize(size);
            }
            AssertArgumentError(
                [&] {
                    return PlanReadBlob(BlockSize, MakeBlobId(), metadata, {0})
                        .GetError();
                });
            AssertArgumentError(
                [&]
                {
                    return DecompressBlob(
                        BlockSize,
                        {},
                        metadata,
                        1500,
                        {0},
                        MakeSgList(output),
                        nullptr);
                });
        }
    }

    Y_UNIT_TEST(ShouldRejectMissingChunkMetadataAndInvalidOutputBuffers)
    {
        TString output(BlockSize, '?');
        NProto::TCompressionMetadata metadata;
        metadata.SetChunkSize(ChunkSize);
        // Missing representation must not be mistaken for a single chunk.
        AssertArgumentError(
            [&] {
                return PlanReadBlob(BlockSize, MakeBlobId(), metadata, {0})
                    .GetError();
            });
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    {},
                    metadata,
                    1500,
                    {0},
                    MakeSgList(output),
                    nullptr);
            });
        metadata.MutablePlainChunks();
        AssertArgumentError(
            [&] {
                return PlanReadBlob(BlockSize, MakeBlobId(0), metadata, {0})
                    .GetError();
            });
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    {},
                    metadata,
                    0,
                    {0},
                    MakeSgList(output),
                    nullptr);
            });
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    {},
                    metadata,
                    1500,
                    {0},
                    {TBlockDataRef::CreateZeroBlock(BlockSize)},
                    nullptr);
            });
    }

    Y_UNIT_TEST(ShouldRejectZeroBlockSizeForReads)
    {
        const auto compression = MakeCompression();

        AssertArgumentError(
            [&] {
                return PlanReadBlob(0, MakeBlobId(), compression, {0})
                    .GetError();
            });

        TString output(BlockSize, '?');
        TVector<ui32> checksums;
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    0,
                    {TRope(TString(100, 'a'))},
                    compression,
                    1500,
                    {0},
                    {{output.begin(), output.size()}},
                    &checksums);
            });
        UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), output);
        UNIT_ASSERT(checksums.empty());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
