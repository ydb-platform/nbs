#include "blob_compressor.h"

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/config.h>

#include <library/cpp/blockcodecs/codecs.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/cast.h>
#include <util/random/fast.h>
#include <util/system/unaligned_mem.h>

#include <initializer_list>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TCompression = NProto::TBlobCompressionInfo;

constexpr ui64 TabletId = 123456789;
constexpr ui32 BlockSize = 4096;
constexpr ui32 ChunkSize = 32 * 1024;

template <typename T>
T GetResultOrFail(TResultOrError<T> result)
{
    UNIT_ASSERT_C(!HasError(result.GetError()), FormatError(result.GetError()));
    return result.ExtractResult();
}

// Stored sizes of the chunks: a raw chunk occupies its uncompressed size.
TVector<ui32> GetChunkSizes(const TCompression& compression, ui32 blobSize)
{
    const auto layout = GetResultOrFail(
        TBlobChunkLayout::Create(compression, blobSize, BlockSize));
    TVector<ui32> sizes;
    for (ui32 i = 0; i < layout.GetChunkCount(); ++i) {
        sizes.push_back(layout.GetStoredChunkLength(i));
    }
    return sizes;
}

ui32 GetChunkSize(const TCompression& compression, ui32 blobSize, size_t chunk)
{
    return GetChunkSizes(compression, blobSize)[chunk];
}

NKikimr::TLogoBlobID MakeBlobId(ui32 size = 1500)
{
    return NKikimr::TLogoBlobID(TabletId, 1, 2, 3, size, 4);
}

const NBlockCodecs::ICodec* GetCodec()
{
    return NBlockCodecs::Codec("lz4-fast14-safe");
}

TRope MakeRope(std::initializer_list<TRope> buffers)
{
    TRope result;
    for (const auto& buffer: buffers) {
        result.Insert(result.End(), TRope(buffer));
    }
    return result;
}

TString DecompressWholeBlob(
    const NProto::TBlobCompressionInfo& compression,
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
        TRope(TString(data)),
        compression,
        data.size(),
        blocks,
        outputs,
        nullptr)));
    return result;
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

void SetRegularChunks(TCompression& compression, ui32 maxSize, ui32 lastSize)
{
    auto* chunks = compression.MutableRegularChunks();
    chunks->SetMaxUncompressedChunkSize(maxSize);
    chunks->SetLastUncompressedChunkSize(lastSize);
}

TCompression MakeCompression()
{
    TCompression compression;
    compression.SetCodec(NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE);
    SetRegularChunks(compression, ChunkSize, ChunkSize);
    for (const ui32 size: {100, 200, 300, 400, 500}) {
        compression.AddCompressedChunkSizes(size);
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
            NCloud::NProto::ECompressionCodec Type;
        };

        const TCase cases[] = {
            {"lz4",
             "lz4-fast14-safe",
             NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE},
            {"snappy", "snappy", NCloud::NProto::COMPRESSION_CODEC_SNAPPY},
            {"zstd", "zstd_3", NCloud::NProto::COMPRESSION_CODEC_ZSTD_3}};
        const TString payload(BlockSize, 'a');
        for (const auto& test: cases) {
            for (const auto name: {test.Family, test.CodecName}) {
                NProto::TStorageServiceConfig config;
                config.SetBlobCompressionCodec(TString(name));
                const auto codec = GetResultOrFail(ParseCompressionCodec(
                    config.GetBlobCompressionCodec()));
                UNIT_ASSERT(test.Type == codec);
                const auto blob = GetResultOrFail(CompressBlob(
                    BlockSize,
                    ChunkSize,
                    codec,
                    MakeSgList(payload)));
                UNIT_ASSERT(test.Type == blob.Info.GetCodec());
                UNIT_ASSERT_VALUES_EQUAL(
                    NBlockCodecs::Codec(test.CodecName)->Encode(payload),
                    blob.Content);
            }
        }
    }

    Y_UNIT_TEST(ShouldValidateCompressionMetadataWithoutReadingBlob)
    {
        auto compression = MakeCompression();
        constexpr ui32 blockCount = 5 * (ChunkSize / BlockSize);
        UNIT_ASSERT(!HasError(ValidateCompressionMetadata(
            BlockSize,
            compression,
            1500,
            blockCount)));
        // The chunks occupy 1500 bytes.
        AssertArgumentError(
            [&]
            {
                return ValidateCompressionMetadata(
                    BlockSize,
                    compression,
                    1000,
                    blockCount);
            });
        compression.SetCodec(NProto::COMPRESSION_CODEC_NONE);
        AssertArgumentError(
            [&]
            {
                return ValidateCompressionMetadata(
                    BlockSize,
                    compression,
                    1500,
                    blockCount);
            });
    }

    Y_UNIT_TEST(ShouldRequireChunksToCoverExactlyThePayloadBlocks)
    {
        auto compression = MakeCompression();
        SetRegularChunks(compression, ChunkSize, BlockSize);
        constexpr ui32 blockCount = 4 * (ChunkSize / BlockSize) + 1;
        auto validate = [&](ui32 expectedBlockCount)
        {
            return ValidateCompressionMetadata(
                BlockSize,
                compression,
                1500,
                expectedBlockCount);
        };
        UNIT_ASSERT(!HasError(validate(blockCount)));
        AssertArgumentError([&] { return validate(blockCount - 1); });
        AssertArgumentError([&] { return validate(blockCount + 1); });

        compression.ClearCompressedChunkSizes();
        compression.AddCompressedChunkSizes(1500);
        UNIT_ASSERT(!HasError(validate(1)));
        AssertArgumentError([&] { return validate(blockCount); });
    }

    Y_UNIT_TEST(ShouldSkipMetricsWhenNotConfigured)
    {
        const TString payload(BlockSize, 'a');
        const TVector<std::optional<TBlobCompressor::TMetricsConfig>> configs =
            {std::nullopt, TBlobCompressor::TMetricsConfig{}};
        for (const auto& config: configs) {
            const TBlobCompressor compressor(
                NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
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
        const ui32 logicalBlockSize = 2 * BlockSize;
        TString payload(3 * logicalBlockSize, '\0');
        payload[2 * logicalBlockSize - 1] = 'a';
        auto counters = std::make_shared<TThreadSafePartCounters>(
            std::make_unique<TPartitionDiskCounters>(
                EPublishingPolicy::Repl,
                diagnostics));
        const TBlobCompressor compressor(
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            TBlobCompressor::TMetricsConfig{
                .CompressionRate = 1,
                .BlockSize = logicalBlockSize,
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
                UNIT_ASSERT_VALUES_EQUAL(
                    2 * logicalBlockSize,
                    values->Cumulative.ZeroBytesWritten.Value);
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
                NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                MakeSgList(payload)));

            UNIT_ASSERT(
                NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE ==
                compression.GetCodec());
            UNIT_ASSERT(compression.HasRegularChunks());
            const auto& chunks = compression.GetRegularChunks();
            UNIT_ASSERT_VALUES_EQUAL(
                chunkSize,
                chunks.GetMaxUncompressedChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                BlockSize,
                chunks.GetLastUncompressedChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(3, compression.CompressedChunkSizesSize());
            UNIT_ASSERT(compressed.size() < payload.size());
            UNIT_ASSERT_VALUES_EQUAL(
                payload,
                DecompressWholeBlob(compression, compressed, payload.size()));

            ui64 offset = 0;
            for (size_t i = 0; i < compression.CompressedChunkSizesSize(); ++i)
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
             {"lz4-fast14-safe", "snappy", "zstd_3"})
        {
            const auto codec =
                GetResultOrFail(ParseCompressionCodec(codecName));
            const auto blob = GetResultOrFail(
                CompressBlob(BlockSize, ChunkSize, codec, MakeSgList(payload)));
            NProto::TBlobCompressionInfo metadata;
            UNIT_ASSERT(
                metadata.ParseFromString(blob.Info.SerializeAsString()));
            UNIT_ASSERT(codec == metadata.GetCodec());

            UNIT_ASSERT_VALUES_EQUAL(
                payload,
                DecompressWholeBlob(metadata, blob.Content, payload.size()));
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
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            MakeSgList(payload)));
        const TVector<ui16> blocks = {1, 2, 16};

        const auto plan = GetResultOrFail(PlanReadBlob(
            BlockSize,
            MakeBlobId(blob.Content.size()),
            blob.Info,
            blocks));
        UNIT_ASSERT_VALUES_EQUAL(2, plan.Queries.size());
        UNIT_ASSERT_VALUES_EQUAL(0, plan.Queries[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(
            GetChunkSize(blob.Info, blob.Content.size(), 0),
            plan.Queries[0].Size);
        UNIT_ASSERT_VALUES_EQUAL(
            GetChunkSize(blob.Info, blob.Content.size(), 0) +
                GetChunkSize(blob.Info, blob.Content.size(), 1),
            plan.Queries[1].Shift);
        UNIT_ASSERT_VALUES_EQUAL(
            GetChunkSize(blob.Info, blob.Content.size(), 2),
            plan.Queries[1].Size);

        TRope inputs;
        for (const auto& query: plan.Queries) {
            inputs.Insert(
                inputs.End(),
                TRope(TString(
                    TStringBuf(blob.Content).SubStr(query.Shift, query.Size))));
        }
        TString result(blocks.size() * BlockSize, '?');
        const auto error = DecompressBlob(
            BlockSize,
            inputs,
            blob.Info,
            blob.Content.size(),
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
                NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                input));
            UNIT_ASSERT_VALUES_EQUAL(
                (blockCount + ChunkSize / BlockSize - 1) /
                    (ChunkSize / BlockSize),
                blob.Info.CompressedChunkSizesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                ((blockCount - 1) % (ChunkSize / BlockSize) + 1) * BlockSize,
                blob.Info.GetRegularChunks().GetLastUncompressedChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                payload,
                DecompressWholeBlob(blob.Info, blob.Content, payload.size()));
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
                               NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                               input)
                        .GetError();
                });
        }
    }

    Y_UNIT_TEST(ShouldRejectEmptyPayloadAndHandleEmptyReads)
    {
        AssertArgumentError(
            [&]
            {
                return CompressBlob(
                           BlockSize,
                           ChunkSize,
                           NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                           {})
                    .GetError();
            });
        const auto [emptyQueries, emptyBlockOffsets] =
            GetResultOrFail(PlanReadBlob(BlockSize, {}, {}, {}));
        UNIT_ASSERT(emptyQueries.empty());
        const auto [queries, blockRangesForEachRequest] = GetResultOrFail(
            PlanReadBlob(BlockSize, MakeBlobId(), MakeCompression(), {}));
        UNIT_ASSERT(queries.empty());
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    {},
                    MakeCompression(),
                    0,
                    {},
                    {},
                    nullptr);
            });
        UNIT_ASSERT(!HasError(DecompressBlob(
            BlockSize,
            MakeRope({{}, {}}),
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
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            MakeSgList(payload)));

        // A chunk which does not shrink is stored raw.
        UNIT_ASSERT_VALUES_EQUAL(3, compression.CompressedChunkSizesSize());
        for (const auto size: compression.GetCompressedChunkSizes()) {
            UNIT_ASSERT_VALUES_EQUAL(0, size);
        }
        UNIT_ASSERT_VALUES_EQUAL(payload, compressed);
        UNIT_ASSERT_VALUES_EQUAL(
            payload,
            DecompressWholeBlob(compression, compressed, payload.size()));
    }

    Y_UNIT_TEST(ShouldReadRawAndCompressedChunksOfDifferentSizes)
    {
        // Chunks of 2, 8 and 1 blocks. The second one is incompressible.
        TFastRng64 rng(123);
        TString payload(2 * BlockSize, 'a');
        for (ui32 i = 0; i < ChunkSize; ++i) {
            payload.push_back(static_cast<char>(rng.GenRand()));
        }
        payload.append(TString(BlockSize, 'b'));

        const auto blob = GetResultOrFail(CompressChunks(
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            BlockSize,
            ChunkSize,
            0,   // minSavedPercent
            payload,
            TVector<ui32>{2 * BlockSize, ChunkSize, BlockSize}));
        UNIT_ASSERT(blob.Info.HasExplicitChunks());
        UNIT_ASSERT_VALUES_EQUAL(0, blob.Info.GetCompressedChunkSizes(1));

        const ui32 blobSize = blob.Content.size();
        const auto sizes = GetChunkSizes(blob.Info, blobSize);
        UNIT_ASSERT_VALUES_EQUAL(ChunkSize, sizes[1]);

        auto check = [&](const TVector<ui16>& blocks,
                         const TVector<std::pair<ui32, ui32>>& expectedQueries)
        {
            const auto plan = GetResultOrFail(PlanReadBlob(
                BlockSize,
                MakeBlobId(blobSize),
                blob.Info,
                blocks));
            UNIT_ASSERT_VALUES_EQUAL(
                expectedQueries.size(),
                plan.Queries.size());

            TRope inputs;
            for (size_t i = 0; i < plan.Queries.size(); ++i) {
                const auto& query = plan.Queries[i];
                UNIT_ASSERT_VALUES_EQUAL(expectedQueries[i].first, query.Shift);
                UNIT_ASSERT_VALUES_EQUAL(expectedQueries[i].second, query.Size);
                inputs.Insert(
                    inputs.End(),
                    TRope(TString(TStringBuf(blob.Content)
                                      .SubStr(query.Shift, query.Size))));
            }

            TString result(blocks.size() * BlockSize, '?');
            TVector<ui32> checksums;
            const auto error = DecompressBlob(
                BlockSize,
                inputs,
                blob.Info,
                blobSize,
                blocks,
                MakeSgList(result),
                &checksums);
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL(blocks.size(), checksums.size());
            for (size_t i = 0; i < blocks.size(); ++i) {
                const auto block = TStringBuf(payload).SubStr(
                    blocks[i] * BlockSize,
                    BlockSize);
                UNIT_ASSERT_VALUES_EQUAL(
                    block,
                    TStringBuf(result).SubStr(i * BlockSize, BlockSize));
                UNIT_ASSERT_VALUES_EQUAL(
                    ComputeDefaultDigest({block.data(), block.size()}),
                    checksums[i]);
            }
        };

        // Adjacent chunks are read by one query.
        check({1, 2, 9, 10}, {{0, blobSize}});
        check({0, 10}, {{0, sizes[0]}, {sizes[0] + sizes[1], sizes[2]}});
        check({5}, {{sizes[0], sizes[1]}});
    }

    Y_UNIT_TEST(ShouldDeduplicateChunksAndCoalesceAdjacentReads)
    {
        // Sorted, distinct blocks in chunks 0, 1 and 4. Chunks 2 and 3
        // must not be read, even though their bytes lie between the queries.
        const auto [queries, blockRangesForEachRequest] =
            GetResultOrFail(PlanReadBlob(
                BlockSize,
                MakeBlobId(),
                MakeCompression(),
                TVector<ui16>{0, 1, 7, 8, 9, 32, 39}));

        UNIT_ASSERT_VALUES_EQUAL(2, queries.size());
        for (const auto& query: queries) {
            UNIT_ASSERT(query.Id == MakeBlobId());
        }
        UNIT_ASSERT_VALUES_EQUAL(0, queries[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(300, queries[0].Size);
        UNIT_ASSERT_VALUES_EQUAL(1000, queries[1].Shift);
        UNIT_ASSERT_VALUES_EQUAL(500, queries[1].Size);
        UNIT_ASSERT_VALUES_EQUAL(2, blockRangesForEachRequest.size());
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange32::MakeClosedInterval(0, 9),
            blockRangesForEachRequest[0]);
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange32::MakeClosedInterval(32, 39),
            blockRangesForEachRequest[1]);
    }

    Y_UNIT_TEST(ShouldPlanSingleAndFullBlobReads)
    {
        const auto compression = MakeCompression();
        const auto [single, singleBlockOffsets] = GetResultOrFail(PlanReadBlob(
            BlockSize,
            MakeBlobId(),
            compression,
            TVector<ui16>{23}));
        UNIT_ASSERT_VALUES_EQUAL(1, single.size());
        UNIT_ASSERT_VALUES_EQUAL(300, single[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(300, single[0].Size);

        const auto [full, fullBlockOffsets] = GetResultOrFail(PlanReadBlob(
            BlockSize,
            MakeBlobId(),
            compression,
            TVector<ui16>{0, 8, 16, 24, 32}));
        UNIT_ASSERT_VALUES_EQUAL(1, full.size());
        UNIT_ASSERT_VALUES_EQUAL(0, full[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(1500, full[0].Size);
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
                        NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                        MakeSgList(payload, blockSize)));
                const TVector<ui16> blocks = {0, 17, 18, 31};
                const auto [queries, blockRangesForEachRequest] =
                    GetResultOrFail(PlanReadBlob(
                        blockSize,
                        MakeBlobId(compressed.size()),
                        compression,
                        blocks));

                TRope inputs;
                for (const auto& query: queries) {
                    inputs.Insert(
                        inputs.End(),
                        TRope(TString(TStringBuf(compressed)
                                          .SubStr(query.Shift, query.Size))));
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
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            MakeSgList(payload)));
        const TVector<ui16> blocks = {8, 9, 17, 18, 32};
        const auto [queries, blockRangesForEachRequest] =
            GetResultOrFail(PlanReadBlob(
                BlockSize,
                MakeBlobId(blob.Content.size()),
                blob.Info,
                blocks));
        UNIT_ASSERT_VALUES_EQUAL(2, queries.size());
        UNIT_ASSERT(queries[0].Shift > 0);
        TRope inputs;
        for (const auto& query: queries) {
            inputs.Insert(
                inputs.End(),
                TRope(TString(
                    TStringBuf(blob.Content).SubStr(query.Shift, query.Size))));
            inputs.Insert(inputs.End(), TRope());
        }

        TVector<TString> results(blocks.size(), TString(BlockSize + 2, '!'));
        TSgList outputs;
        for (auto& result: results) {
            outputs.push_back({result.begin() + 1, BlockSize});
        }
        UNIT_ASSERT(!HasError(DecompressBlob(
            BlockSize,
            inputs,
            blob.Info,
            blob.Content.size(),
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
                NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                MakeSgList(payload)));
            NProto::TBlobCompressionInfo metadata;
            UNIT_ASSERT(
                metadata.ParseFromString(blob.Info.SerializeAsString()));
            const auto& chunks = metadata.GetRegularChunks();
            UNIT_ASSERT_VALUES_EQUAL(
                chunkSize,
                chunks.GetMaxUncompressedChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                BlockSize,
                chunks.GetLastUncompressedChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(4, metadata.CompressedChunkSizesSize());

            const TVector<ui16> blocks = {
                static_cast<ui16>(blocksPerChunk),
                static_cast<ui16>(blocksPerChunk + 1),
                lastBlock};
            const auto [queries, blockRangesForEachRequest] =
                GetResultOrFail(PlanReadBlob(
                    BlockSize,
                    MakeBlobId(blob.Content.size()),
                    metadata,
                    blocks));
            UNIT_ASSERT_VALUES_EQUAL(2, queries.size());
            UNIT_ASSERT_VALUES_EQUAL(
                GetChunkSize(metadata, blob.Content.size(), 0),
                queries[0].Shift);
            UNIT_ASSERT_VALUES_EQUAL(
                GetChunkSize(metadata, blob.Content.size(), 1),
                queries[0].Size);
            UNIT_ASSERT_VALUES_EQUAL(
                GetChunkSize(metadata, blob.Content.size(), 0) +
                    GetChunkSize(metadata, blob.Content.size(), 1) +
                    GetChunkSize(metadata, blob.Content.size(), 2),
                queries[1].Shift);
            UNIT_ASSERT_VALUES_EQUAL(
                GetChunkSize(metadata, blob.Content.size(), 3),
                queries[1].Size);

            TRope inputs;
            for (const auto& query: queries) {
                inputs.Insert(
                    inputs.End(),
                    TRope(TString(TStringBuf(blob.Content).SubStr(
                        query.Shift,
                        query.Size))));
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
                blob.Content.size(),
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
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            MakeSgList(payload)));
        const TVector<ui16> blocks = {1, 8, 9, 16};
        for (const bool splitEveryChunk: {false, true}) {
            TRope input;
            size_t offset = 0;
            for (const auto size: GetChunkSizes(blob.Info, blob.Content.size()))
            {
                const auto chunk =
                    TStringBuf(blob.Content).SubStr(offset, size);
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
                GetChunkSize(blob.Info, blob.Content.size(), 0);
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
                const TRope inputs = MakeRope(
                    {{},
                     TRope(input.Begin(), input.Position(split)),
                     {},
                     TRope(input.Position(split), input.End()),
                     {}});
                for (auto& result: results) {
                    std::fill_n(result.begin(), result.size(), '?');
                }
                const auto error = DecompressBlob(
                    BlockSize,
                    inputs,
                    blob.Info,
                    blob.Content.size(),
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
            UNIT_ASSERT_VALUES_EQUAL(blob.Content, input.ConvertToString());
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidChunkSizeMetadata)
    {
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            MakeSgList(TString(ChunkSize, 'a'))));
        TString result(BlockSize, '?');
        const TSgList outputs = {{result.begin(), result.size()}};
        for (const ui32 size: {0u, BlockSize / 2, BlockSize - 1, BlockSize + 1})
        {
            auto metadata = blob.Info;
            SetRegularChunks(metadata, size, ChunkSize);
            AssertArgumentError(
                [&]
                {
                    return PlanReadBlob(
                               BlockSize,
                               MakeBlobId(blob.Content.size()),
                               metadata,
                               TVector<ui16>{0})
                        .GetError();
                });
            AssertArgumentError(
                [&]
                {
                    return DecompressBlob(
                        BlockSize,
                        TRope(blob.Content),
                        metadata,
                        blob.Content.size(),
                        TVector<ui16>{0},
                        outputs,
                        nullptr);
                });
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidLastChunkSizeMetadata)
    {
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            MakeSgList(TString(BlockSize, 'a'))));
        TString output(BlockSize, '?');
        for (const ui32 size:
             {0u, BlockSize - 1, BlockSize + 1, ChunkSize + BlockSize})
        {
            auto metadata = blob.Info;
            SetRegularChunks(metadata, ChunkSize, size);
            AssertArgumentError(
                [&]
                {
                    return PlanReadBlob(
                               BlockSize,
                               MakeBlobId(blob.Content.size()),
                               metadata,
                               TVector<ui16>{0})
                        .GetError();
                });
            AssertArgumentError(
                [&]
                {
                    return DecompressBlob(
                        BlockSize,
                        {},
                        metadata,
                        blob.Content.size(),
                        TVector<ui16>{0},
                        MakeSgList(output),
                        nullptr);
                });
        }
        UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), output);
    }

    Y_UNIT_TEST(ShouldRejectReadsBeyondPartialLastChunkBeforeDecoding)
    {
        for (const ui32 fullChunks: {0, 2}) {
            for (const ui32 lastBlocks: {1u, 3u, ChunkSize / BlockSize}) {
                const ui32 blockCount =
                    fullChunks * (ChunkSize / BlockSize) + lastBlocks;
                const auto blob = GetResultOrFail(CompressBlob(
                    BlockSize,
                    ChunkSize,
                    NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                    MakeSgList(TString(blockCount * BlockSize, 'a'))));
                const auto blobId = MakeBlobId(blob.Content.size());
                GetResultOrFail(PlanReadBlob(
                    BlockSize,
                    blobId,
                    blob.Info,
                    TVector<ui16>{static_cast<ui16>(blockCount - 1)}));
                TString output(BlockSize, '?');
                for (const ui16 offset:
                     {static_cast<ui16>(blockCount),
                      static_cast<ui16>(
                          Max(blockCount,
                              (fullChunks + 1) * (ChunkSize / BlockSize) - 1))})
                {
                    AssertArgumentError(
                        [&]
                        {
                            return PlanReadBlob(
                                       BlockSize,
                                       blobId,
                                       blob.Info,
                                       TVector<ui16>{offset})
                                .GetError();
                        });
                    // No compressed data is needed to reject this offset.
                    AssertArgumentError(
                        [&]
                        {
                            return DecompressBlob(
                                BlockSize,
                                {},
                                blob.Info,
                                blob.Content.size(),
                                TVector<ui16>{offset},
                                MakeSgList(output),
                                nullptr);
                        });
                }
                UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), output);
            }
        }
    }

    Y_UNIT_TEST(ShouldRejectLastChunkSizeMismatch)
    {
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            MakeSgList(TString(2 * BlockSize, 'a'))));
        for (const ui32 size: {BlockSize, 3 * BlockSize}) {
            auto metadata = blob.Info;
            SetRegularChunks(metadata, ChunkSize, size);
            TString output(BlockSize, '?');
            const auto error = DecompressBlob(
                BlockSize,
                TRope(blob.Content),
                metadata,
                blob.Content.size(),
                TVector<ui16>{0},
                MakeSgList(output),
                nullptr);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), output);
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidDecompressionBuffers)
    {
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            MakeSgList(TString(ChunkSize, 'a'))));
        TString result(BlockSize + 1, '?');
        const TSgList outputs = {{result.begin(), BlockSize}};
        const auto data = TStringBuf(blob.Content);

        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    TRope(TString(data)),
                    blob.Info,
                    blob.Content.size(),
                    TVector<ui16>{0},
                    {},
                    nullptr);
            });
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    TRope(TString(data)),
                    blob.Info,
                    blob.Content.size(),
                    TVector<ui16>{0},
                    TSgList{{result.begin(), BlockSize - 1}},
                    nullptr);
            });
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    TRope(TString(data)),
                    blob.Info,
                    blob.Content.size(),
                    TVector<ui16>{0},
                    TSgList{{result.begin(), BlockSize + 1}},
                    nullptr);
            });
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                {},
                blob.Info,
                blob.Content.size(),
                TVector<ui16>{0},
                outputs,
                nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                MakeRope({{}, {}}),
                blob.Info,
                blob.Content.size(),
                TVector<ui16>{0},
                outputs,
                nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                TRope(TString(data.Head(data.size() - 1))),
                blob.Info,
                blob.Content.size(),
                TVector<ui16>{0},
                outputs,
                nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                MakeRope({TRope(TString(data)), TRope(TString("extra"))}),
                blob.Info,
                blob.Content.size(),
                TVector<ui16>{0},
                outputs,
                nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                TRope(TString(data)),
                blob.Info,
                blob.Content.size(),
                {},
                {},
                nullptr)
                .GetCode());
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    TRope(TString(data)),
                    blob.Info,
                    blob.Content.size(),
                    TVector<ui16>{8},
                    outputs,
                    nullptr);
            });
    }

    Y_UNIT_TEST(ShouldRejectTruncatedChunksAcrossRopes)
    {
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            MakeSgList(TString(ChunkSize, 'a') + TString(BlockSize, 'b'))));
        const auto data = TStringBuf(blob.Content);
        const size_t firstChunkSize =
            GetChunkSize(blob.Info, blob.Content.size(), 0);
        const TVector<size_t> sizes = {
            firstChunkSize - 1,
            firstChunkSize,
            data.size() - 1};
        TString result(2 * BlockSize, '?');
        for (const auto size: sizes) {
            const auto error = DecompressBlob(
                BlockSize,
                MakeRope(
                    {{},
                     TRope(TString(data.Head(1))),
                     {},
                     TRope(TString(data.SubStr(1, size - 1))),
                     {}}),
                blob.Info,
                blob.Content.size(),
                TVector<ui16>{0, ChunkSize / BlockSize},
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
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            MakeSgList(TString(BlockSize, 'a'))));
        // The header claims a full chunk, but LZ4 produces only one block.
        WriteUnaligned<ui64>(blob.Content.begin(), ChunkSize);

        TString result(BlockSize, '?');
        const auto error = DecompressBlob(
            BlockSize,
            TRope(blob.Content),
            blob.Info,
            blob.Content.size(),
            TVector<ui16>{0},
            TSgList{{result.begin(), result.size()}},
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
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            MakeSgList(TString(ChunkSize, 'a'))));
        TString result(BlockSize, '?');
        const TSgList outputs = {{result.begin(), result.size()}};

        auto metadata = blob.Info;
        metadata.AddCompressedChunkSizes(0);
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    TRope(blob.Content),
                    metadata,
                    blob.Content.size(),
                    TVector<ui16>{0},
                    outputs,
                    nullptr);
            });
        for (const int value:
             {-1, 0, NCloud::NProto::ECompressionCodec_MAX + 1, Max<int>()})
        {
            const auto codec =
                static_cast<NCloud::NProto::ECompressionCodec>(value);
            metadata = blob.Info;
            metadata.SetCodec(codec);
            AssertArgumentError(
                [&]
                {
                    return PlanReadBlob(
                               BlockSize,
                               MakeBlobId(blob.Content.size()),
                               metadata,
                               TVector<ui16>{0})
                        .GetError();
                });
            AssertArgumentError(
                [&]
                {
                    return DecompressBlob(
                        BlockSize,
                        TRope(blob.Content),
                        metadata,
                        blob.Content.size(),
                        TVector<ui16>{0},
                        outputs,
                        nullptr);
                });
        }

        // Reject zero decoded lengths and corrupt compressed payloads.
        TString emptyChunk = blob.Content;
        WriteUnaligned<ui64>(emptyChunk.begin(), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                TRope(emptyChunk),
                blob.Info,
                blob.Content.size(),
                TVector<ui16>{0},
                outputs,
                nullptr)
                .GetCode());
        for (const ui64 size:
             {ui64(BlockSize - 1), ui64(ChunkSize + BlockSize), Max<ui64>()})
        {
            TString corrupt = blob.Content;
            WriteUnaligned<ui64>(corrupt.begin(), size);
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                DecompressBlob(
                    BlockSize,
                    TRope(corrupt),
                    blob.Info,
                    blob.Content.size(),
                    TVector<ui16>{0},
                    outputs,
                    nullptr)
                    .GetCode());
        }

        // A valid length header without any compressed payload is corrupt.
        TString header(sizeof(ui64), '\0');
        WriteUnaligned<ui64>(header.begin(), ChunkSize);
        metadata = blob.Info;
        metadata.SetCompressedChunkSizes(0, header.size());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                TRope(header),
                metadata,
                header.size(),
                TVector<ui16>{0},
                outputs,
                nullptr)
                .GetCode());

        // A truncated length header must also return a decoding error.
        metadata.SetCompressedChunkSizes(0, sizeof(ui64) - 1);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                TRope(TString(header.data(), sizeof(ui64) - 1)),
                metadata,
                sizeof(ui64) - 1,
                TVector<ui16>{0},
                outputs,
                nullptr)
                .GetCode());

        // Only the final chunk is allowed to be shorter than ChunkSize.
        const auto partial = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            MakeSgList(TString(BlockSize, 'a'))));
        metadata = partial.Info;
        metadata.AddCompressedChunkSizes(partial.Content.size());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                TRope(partial.Content),
                metadata,
                2 * partial.Content.size(),
                TVector<ui16>{0},
                outputs,
                nullptr)
                .GetCode());
    }

    Y_UNIT_TEST(ShouldRejectInvalidChunkSizes)
    {
        const TString payload(BlockSize, 'a');
        for (const ui32 chunkSize: {BlockSize / 2, BlockSize + 1}) {
            AssertArgumentError(
                [&]
                {
                    return CompressBlob(
                               BlockSize,
                               chunkSize,
                               NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                               MakeSgList(payload))
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
        NProto::TBlobCompressionInfo compression;
        compression.SetCodec(NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE);
        SetRegularChunks(compression, chunkSize, chunkSize);
        for (ui32 i = 0; i < (static_cast<ui32>(blockOffset) + 1) / 2; ++i) {
            compression.AddCompressedChunkSizes(compressedChunkSize);
        }

        const auto [queries, blockRangesForEachRequest] =
            GetResultOrFail(PlanReadBlob(
                blockSize,
                MakeBlobId(
                    (static_cast<ui32>(blockOffset) + 1) / 2 *
                    compressedChunkSize),
                compression,
                TVector<ui16>{blockOffset}));
        UNIT_ASSERT_VALUES_EQUAL(1, queries.size());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(blockOffset - 1) / 2 * compressedChunkSize,
            queries[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(compressedChunkSize, queries[0].Size);
    }

    Y_UNIT_TEST(ShouldRejectReadsBeyondBlobSize)
    {
        const auto compression = MakeCompression();
        const NKikimr::TLogoBlobID blobId(TabletId, 1, 2, 3, 99, 4);

        // The chunks occupy more bytes than the blob has. No query can be sent
        // to storage.
        for (const ui16 blockOffset: {0, 8}) {
            AssertArgumentError(
                [&]
                {
                    return PlanReadBlob(
                               BlockSize,
                               blobId,
                               compression,
                               TVector<ui16>{blockOffset})
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
                           NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                           {})
                    .GetError();
            });
        const TString payload(BlockSize, 'a');
        AssertArgumentError(
            [&]
            {
                return CompressBlob(
                           BlockSize,
                           0,
                           NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                           MakeSgList(payload))
                    .GetError();
            });

        AssertArgumentError(
            [&]
            {
                return CompressBlob(
                           BlockSize,
                           ChunkSize,
                           NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                           TSgList{{"unaligned", 9}})
                    .GetError();
            });
        AssertArgumentError(
            [&]
            {
                return PlanReadBlob(
                           BlockSize,
                           MakeBlobId(),
                           MakeCompression(),
                           TVector<ui16>{40})
                    .GetError();
            });
        AssertArgumentError(
            [&] {
                return PlanReadBlob(BlockSize, {}, {}, TVector<ui16>{0})
                    .GetError();
            });

        auto compression = MakeCompression();
        compression.SetCompressedChunkSizes(2, 0);
        AssertArgumentError(
            [&]
            {
                return PlanReadBlob(
                           BlockSize,
                           MakeBlobId(),
                           compression,
                           TVector<ui16>{16})
                    .GetError();
            });
    }

    Y_UNIT_TEST(ShouldReadSingleChunkAndCalculateChecksums)
    {
        const TString payload =
            TString(BlockSize, 'a') + TString(BlockSize, 'b');
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            MakeSgList(payload)));
        UNIT_ASSERT_VALUES_EQUAL(1, blob.Info.CompressedChunkSizesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            blob.Content.size(),
            blob.Info.GetCompressedChunkSizes(0));

        const TVector<ui16> blocks = {0, 1};
        const auto plan = GetResultOrFail(PlanReadBlob(
            BlockSize,
            MakeBlobId(blob.Content.size()),
            blob.Info,
            blocks));
        UNIT_ASSERT_VALUES_EQUAL(1, plan.Queries.size());
        UNIT_ASSERT_VALUES_EQUAL(0, plan.Queries[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(blob.Content.size(), plan.Queries[0].Size);

        TString result(blocks.size() * BlockSize, '?');
        TVector<ui32> checksums;
        const auto error = DecompressBlob(
            BlockSize,
            TRope(blob.Content),
            blob.Info,
            blob.Content.size(),
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
        // The stored chunks must occupy exactly the 1500 bytes of the blob. A
        // zero size is a raw chunk which occupies ChunkSize bytes.
        const TVector<TVector<ui32>> invalidSizes = {
            {},
            {0},
            {1499},
            {1501},
            {100, 1401},
            {1500, 0},
            {Max<ui32>()},
            {1, Max<ui32>()}};
        for (const auto& sizes: invalidSizes) {
            TCompression metadata;
            metadata.SetCodec(
                NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE);
            SetRegularChunks(metadata, ChunkSize, ChunkSize);
            for (const auto size: sizes) {
                metadata.AddCompressedChunkSizes(size);
            }
            AssertArgumentError(
                [&]
                {
                    return PlanReadBlob(
                               BlockSize,
                               MakeBlobId(),
                               metadata,
                               TVector<ui16>{0})
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
                        TVector<ui16>{0},
                        MakeSgList(output),
                        nullptr);
                });
        }
    }

    Y_UNIT_TEST(ShouldRejectMissingChunkMetadataAndInvalidOutputBuffers)
    {
        TString output(BlockSize, '?');
        NProto::TBlobCompressionInfo metadata;
        metadata.SetCodec(NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE);
        metadata.AddCompressedChunkSizes(1500);
        // A missing uncompressed layout must not be mistaken for a single
        // chunk.
        AssertArgumentError(
            [&]
            {
                return PlanReadBlob(
                           BlockSize,
                           MakeBlobId(),
                           metadata,
                           TVector<ui16>{0})
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
                    TVector<ui16>{0},
                    MakeSgList(output),
                    nullptr);
            });
        SetRegularChunks(metadata, ChunkSize, ChunkSize);
        AssertArgumentError(
            [&]
            {
                return PlanReadBlob(
                           BlockSize,
                           MakeBlobId(0),
                           metadata,
                           TVector<ui16>{0})
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
                    TVector<ui16>{0},
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
                    TVector<ui16>{0},
                    TSgList{TBlockDataRef::CreateZeroBlock(BlockSize)},
                    nullptr);
            });
    }

    Y_UNIT_TEST(ShouldRejectZeroBlockSizeForReads)
    {
        const auto compression = MakeCompression();

        AssertArgumentError(
            [&]
            {
                return PlanReadBlob(
                           0,
                           MakeBlobId(),
                           compression,
                           TVector<ui16>{0})
                    .GetError();
            });

        TString output(BlockSize, '?');
        TVector<ui32> checksums;
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    0,
                    TRope(TString(100, 'a')),
                    compression,
                    1500,
                    TVector<ui16>{0},
                    TSgList{{output.begin(), output.size()}},
                    &checksums);
            });
        UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), output);
        UNIT_ASSERT(checksums.empty());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
