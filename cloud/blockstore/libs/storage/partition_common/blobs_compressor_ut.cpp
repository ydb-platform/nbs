#include "blobs_compressor.h"

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

constexpr ui64 TabletId = 123456789;
constexpr ui32 BlockSize = 4096;
constexpr ui32 ChunkSize = 32 * 1024;

ui32 GetChunkSize(
    const NProto::TCompressionMetadata& compression,
    ui32 blobSize,
    size_t chunk)
{
    const auto& offsets = compression.GetPlainChuncks();
    return (chunk < offsets.ChunkOffsetSize() ? offsets.GetChunkOffset(chunk)
                                              : blobSize) -
           (chunk ? offsets.GetChunkOffset(chunk - 1) : 0);
}

TVector<ui32> GetChunkSizes(
    const NProto::TCompressionMetadata& compression,
    ui32 blobSize)
{
    TVector<ui32> sizes;
    for (size_t i = 0; i <= compression.GetPlainChuncks().ChunkOffsetSize();
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

TString DecompressBlob(
    const TBlobsCompressor& compressor,
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
    UNIT_ASSERT(!HasError(compressor.DecompressBlob(
        {TRope(TString(data))},
        outputs,
        compression,
        data.size(),
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
    compression.SetCodec("lz4-fast14-safe");
    compression.SetChunkSize(ChunkSize);
    for (ui32 offset: {100, 300, 600, 1000}) {
        compression.MutablePlainChuncks()->AddChunkOffset(offset);
    }
    return compression;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlobsCompressorTest)
{
    Y_UNIT_TEST(ShouldSkipMetricsWhenNotConfigured)
    {
        const TString payload(BlockSize, 'a');
        const TVector<std::optional<TBlobsCompressor::TMetricsConfig>> configs =
            {std::nullopt, TBlobsCompressor::TMetricsConfig{}};
        for (const auto& config: configs) {
            const TBlobsCompressor writer(
                BlockSize,
                ChunkSize,
                "lz4-fast14-safe",
                config);
            const TBlobsCompressor reader(
                BlockSize,
                "lz4-fast14-safe",
                config);
            // Both forms disable sampling and need no counters.
            writer.CalculateCompressionMetrics({}, payload);
            reader.CalculateCompressionMetrics({}, payload);
            const auto blob =
                GetResultOrFail(writer.CompressBlob(MakeSgList(payload)));
            UNIT_ASSERT_VALUES_EQUAL(
                payload,
                DecompressBlob(
                    reader,
                    blob.Compression,
                    blob.Data,
                    payload.size()));
        }
    }

    Y_UNIT_TEST(ShouldCalculateCompressionMetricsWithBothConstructors)
    {
        const TVector<ui64> chunkSizes = {2 * BlockSize, BlockSize, BlockSize};
        NProto::TDiagnosticsConfig proto;
        for (const auto size: chunkSizes) {
            proto.AddBlobCompressionChunkSizes(size);
        }
        const TDiagnosticsConfig diagnostics(proto);
        const TString payload(2 * BlockSize, 'a');

        for (const bool canWrite: {false, true}) {
            auto counters = std::make_shared<TThreadSafePartCounters>(
                std::make_unique<TPartitionDiskCounters>(
                    EPublishingPolicy::Repl,
                    diagnostics));
            const TBlobsCompressor::TMetricsConfig metricsConfig{
                .CompressionRate = 1,
                .ChunkSizes = chunkSizes,
                .PartCounters = counters};
            const auto compressor = canWrite
                                        ? TBlobsCompressor(
                                              BlockSize,
                                              ChunkSize,
                                              "lz4-fast14-safe",
                                              metricsConfig)
                                        : TBlobsCompressor(
                                              BlockSize,
                                              "lz4-fast14-safe",
                                              metricsConfig);

            auto compressed = compressor.CompressBlob(MakeSgList(payload));
            if (canWrite) {
                UNIT_ASSERT_C(
                    !HasError(compressed.GetError()),
                    FormatError(compressed.GetError()));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    E_ARGUMENT,
                    compressed.GetError().GetCode());
            }

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
                    for (const auto& [size, compression]:
                         values->BlobCompression) {
                        // Repeated chunk sizes must be counted only once.
                        UNIT_ASSERT_VALUES_EQUAL(
                            payload.size(),
                            compression.UncompressedBytesWritten.Value);
                        UNIT_ASSERT(
                            compression.CompressedBytesWritten.Value > 0);
                        UNIT_ASSERT(
                            compression.CompressedBytesWritten.Value <
                            payload.size());
                    }
                });
        }
    }

    Y_UNIT_TEST(ShouldReturnCompressedChunksAndMetadata)
    {
        for (const ui32 chunkSize: {16 * 1024, 64 * 1024}) {
            const TBlobsCompressor compressor(
                BlockSize,
                chunkSize,
                "lz4-fast14-safe",
                std::nullopt);
            // Two complete chunks followed by one block.
            const TString payload = TString(chunkSize, 'a') +
                                    TString(chunkSize, 'b') +
                                    TString(BlockSize, 'c');
            const auto [compressed, compression] =
                GetResultOrFail(compressor.CompressBlob(MakeSgList(payload)));

            UNIT_ASSERT_VALUES_EQUAL("lz4-fast14-safe", compression.GetCodec());
            UNIT_ASSERT_VALUES_EQUAL(chunkSize, compression.GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                2,
                compression.GetPlainChuncks().ChunkOffsetSize());
            UNIT_ASSERT(compressed.size() < payload.size());
            UNIT_ASSERT_VALUES_EQUAL(
                payload,
                DecompressBlob(
                    compressor,
                    compression,
                    compressed,
                    payload.size()));

            ui64 offset = 0;
            for (size_t i = 0;
                 i <= compression.GetPlainChuncks().ChunkOffsetSize();
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

    Y_UNIT_TEST(ShouldReadUsingSerializedCodecName)
    {
        const TString payload =
            TString(ChunkSize, 'a') + TString(BlockSize, 'b');
        for (const TStringBuf codecName: {"lz4", "snappy"}) {
            const TBlobsCompressor writer(
                BlockSize,
                ChunkSize,
                codecName,
                std::nullopt);
            const auto blob =
                GetResultOrFail(writer.CompressBlob(MakeSgList(payload)));
            NProto::TCompressionMetadata metadata;
            UNIT_ASSERT(
                metadata.ParseFromString(blob.Compression.SerializeAsString()));
            UNIT_ASSERT_VALUES_EQUAL(
                codecName == "lz4" ? "lz4-fast14-safe" : "snappy",
                metadata.GetCodec());

            // The reader's configured codec must not override blob metadata.
            const TBlobsCompressor reader(
                BlockSize,
                codecName == "lz4" ? "snappy" : "lz4",
                std::nullopt);
            UNIT_ASSERT_VALUES_EQUAL(
                payload,
                DecompressBlob(reader, metadata, blob.Data, payload.size()));
        }
    }

    Y_UNIT_TEST(ShouldCompressSeparateBlockBuffers)
    {
        const TBlobsCompressor compressor(
            BlockSize,
            ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
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

            const auto blob = GetResultOrFail(compressor.CompressBlob(input));
            UNIT_ASSERT_VALUES_EQUAL(
                (blockCount + ChunkSize / BlockSize - 1) /
                        (ChunkSize / BlockSize) -
                    1,
                blob.Compression.GetPlainChuncks().ChunkOffsetSize());
            UNIT_ASSERT_VALUES_EQUAL(
                payload,
                DecompressBlob(
                    compressor,
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
        const TBlobsCompressor compressor(
            BlockSize,
            ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        for (const auto& input: inputs) {
            AssertArgumentError(
                [&] { return compressor.CompressBlob(input).GetError(); });
        }
    }

    Y_UNIT_TEST(ShouldHandleEmptyPayloadAndReads)
    {
        const TBlobsCompressor compressor(
            BlockSize,
            ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        const auto [compressed, compression] =
            GetResultOrFail(compressor.CompressBlob({}));

        UNIT_ASSERT(compressed.empty());
        UNIT_ASSERT_VALUES_EQUAL("lz4-fast14-safe", compression.GetCodec());
        UNIT_ASSERT_VALUES_EQUAL(ChunkSize, compression.GetChunkSize());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            compression.GetPlainChuncks().ChunkOffsetSize());
        const auto [emptyQueries, emptyCount, emptyBlockOffsets] =
            GetResultOrFail(compressor.PlanReadBlob({}, {}, {}));
        UNIT_ASSERT(!emptyQueries);
        UNIT_ASSERT_VALUES_EQUAL(0, emptyCount);
        for (const auto& metadata: {compression, MakeCompression()}) {
            const auto [queries, queriesCount, blockOffsetsForEachRequest] =
                GetResultOrFail(
                    compressor.PlanReadBlob(MakeBlobId(), metadata, {}));
            UNIT_ASSERT(!queries);
            UNIT_ASSERT_VALUES_EQUAL(0, queriesCount);
        }
        UNIT_ASSERT(!HasError(
            compressor.DecompressBlob({}, {}, compression, 0, {}, nullptr)));
        UNIT_ASSERT(!HasError(compressor.DecompressBlob(
            {{}, {}},
            {},
            MakeCompression(),
            1500,
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

        const TBlobsCompressor compressor(
            BlockSize,
            ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        const auto [compressed, compression] =
            GetResultOrFail(compressor.CompressBlob(MakeSgList(payload)));

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            compression.GetPlainChuncks().ChunkOffsetSize());
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
        const TBlobsCompressor compressor(
            BlockSize,
            ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        // Sorted, distinct blocks in chunks 0, 1 and 4. Chunks 2 and 3
        // must not be read, even though their bytes lie between the queries.
        const auto [queries, queriesCount, blockOffsetsForEachRequest] =
            GetResultOrFail(compressor.PlanReadBlob(
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
        const TBlobsCompressor compressor(
            BlockSize,
            ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        const auto compression = MakeCompression();
        const auto [single, singleCount, singleBlockOffsets] = GetResultOrFail(
            compressor.PlanReadBlob(MakeBlobId(), compression, {23}));
        UNIT_ASSERT_VALUES_EQUAL(1, singleCount);
        UNIT_ASSERT_VALUES_EQUAL(300, single[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(300, single[0].Size);

        const auto [full, fullCount, fullBlockOffsets] =
            GetResultOrFail(compressor.PlanReadBlob(
                MakeBlobId(),
                compression,
                {0, 8, 16, 24, 32}));
        UNIT_ASSERT_VALUES_EQUAL(1, fullCount);
        UNIT_ASSERT_VALUES_EQUAL(0, full[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(1500, full[0].Size);
    }

    Y_UNIT_TEST(ShouldRejectUnsortedBlockOffsets)
    {
        const TBlobsCompressor compressor(
            BlockSize,
            ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        const auto blob = GetResultOrFail(
            compressor.CompressBlob(MakeSgList(TString(ChunkSize, 'a'))));
        const TVector<ui16> blocks = {1, 0};
        TString result(2 * BlockSize, '?');
        const TSgList outputs = {
            {result.begin(), BlockSize},
            {result.begin() + BlockSize, BlockSize}};

        AssertArgumentError(
            [&]
            {
                return compressor
                    .PlanReadBlob(
                        MakeBlobId(blob.Data.size()),
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
                    blob.Data.size(),
                    blocks,
                    nullptr);
            });
    }

    Y_UNIT_TEST(ShouldReadRequestedBlocksFromCompressedPayload)
    {
        for (const ui32 blocksPerChunk: {2, 4, 8}) {
            for (const ui32 blockSize: {512, 4096, 65536, 131072}) {
                const ui32 chunkSize = blocksPerChunk * blockSize;
                const TBlobsCompressor compressor(
                    blockSize,
                    chunkSize,
                    "lz4-fast14-safe",
                    std::nullopt);
                TString payload;
                for (ui32 i = 0; i < 32; ++i) {
                    payload.append(TString(blockSize, 'a' + i));
                }
                const auto [compressed, compression] = GetResultOrFail(
                    compressor.CompressBlob(MakeSgList(payload, blockSize)));
                const TVector<ui16> blocks = {0, 17, 18, 31};
                const auto [queries, queriesCount, blockOffsetsForEachRequest] =
                    GetResultOrFail(compressor.PlanReadBlob(
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
                UNIT_ASSERT(!HasError(compressor.DecompressBlob(
                    inputs,
                    outputs,
                    compression,
                    compressed.size(),
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
        // The reader uses a different chunk size.
        // The first requested chunk is not chunk zero.
        const TBlobsCompressor reader(
            BlockSize,
            2 * ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        const TBlobsCompressor writer(
            BlockSize,
            ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        TString payload;
        for (ui32 i = 0; i < 33; ++i) {
            payload.append(TString(BlockSize, 'a' + i));
        }
        const auto blob =
            GetResultOrFail(writer.CompressBlob(MakeSgList(payload)));
        const TVector<ui16> blocks = {8, 9, 17, 18, 32};
        const auto [queries, queriesCount, blockOffsetsForEachRequest] =
            GetResultOrFail(reader.PlanReadBlob(
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
        UNIT_ASSERT(!HasError(reader.DecompressBlob(
            inputs,
            outputs,
            blob.Compression,
            blob.Data.size(),
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

    Y_UNIT_TEST(ShouldPlanAndDecompressWithDifferentChunkSizes)
    {
        for (const ui32 chunkSize: {2 * BlockSize, 4 * ChunkSize}) {
            const ui32 blocksPerChunk = chunkSize / BlockSize;
            const ui16 lastBlock = 3 * blocksPerChunk;
            TString payload;
            for (ui32 i = 0; i <= lastBlock; ++i) {
                payload.append(TString(BlockSize, static_cast<char>(i)));
            }
            const TBlobsCompressor writer(
                BlockSize,
                chunkSize,
                "lz4-fast14-safe",
                std::nullopt);
            const auto blob =
                GetResultOrFail(writer.CompressBlob(MakeSgList(payload)));
            NProto::TCompressionMetadata metadata;
            UNIT_ASSERT(
                metadata.ParseFromString(blob.Compression.SerializeAsString()));
            UNIT_ASSERT_VALUES_EQUAL(chunkSize, metadata.GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                3,
                metadata.GetPlainChuncks().ChunkOffsetSize());

            for (const ui32 readerChunkSize: {4 * BlockSize, 2 * ChunkSize}) {
                const TBlobsCompressor reader(
                    BlockSize,
                    readerChunkSize,
                    "lz4-fast14-safe",
                    std::nullopt);
                const TVector<ui16> blocks = {
                    static_cast<ui16>(blocksPerChunk),
                    static_cast<ui16>(blocksPerChunk + 1),
                    lastBlock};
                const auto [queries, queriesCount, blockOffsetsForEachRequest] =
                    GetResultOrFail(reader.PlanReadBlob(
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
                for (const auto& query:
                     MakeArrayRef(queries.Get(), queriesCount))
                {
                    inputs.emplace_back(TString(
                        TStringBuf(blob.Data).SubStr(query.Shift, query.Size)));
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
                    blob.Data.size(),
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

    Y_UNIT_TEST(ShouldReadFragmentedRopes)
    {
        const TBlobsCompressor compressor(
            BlockSize,
            ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        TString payload;
        for (ui32 i = 0; i < 17; ++i) {
            payload.append(TString(BlockSize, 'a' + i));
        }
        const auto blob =
            GetResultOrFail(compressor.CompressBlob(MakeSgList(payload)));
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
            UNIT_ASSERT(!HasError(compressor.DecompressBlob(
                {{}, input, {}},
                outputs,
                blob.Compression,
                blob.Data.size(),
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

    Y_UNIT_TEST(ShouldRejectInvalidChunkSizeMetadata)
    {
        const TBlobsCompressor compressor(
            BlockSize,
            ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        const auto blob = GetResultOrFail(
            compressor.CompressBlob(MakeSgList(TString(ChunkSize, 'a'))));
        TString result(BlockSize, '?');
        const TSgList outputs = {{result.begin(), result.size()}};
        for (const ui32 size: {0u, BlockSize / 2, BlockSize - 1, BlockSize + 1})
        {
            auto metadata = blob.Compression;
            metadata.SetChunkSize(size);
            AssertArgumentError(
                [&]
                {
                    return compressor
                        .PlanReadBlob(
                            MakeBlobId(blob.Data.size()),
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
                        blob.Data.size(),
                        {0},
                        nullptr);
                });
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidDecompressionBuffers)
    {
        const TBlobsCompressor compressor(
            BlockSize,
            ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        const auto blob = GetResultOrFail(
            compressor.CompressBlob(MakeSgList(TString(ChunkSize, 'a'))));
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
                    blob.Data.size(),
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
                    blob.Data.size(),
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
                    blob.Data.size(),
                    {0},
                    nullptr);
            });
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            compressor
                .DecompressBlob(
                    {},
                    outputs,
                    blob.Compression,
                    blob.Data.size(),
                    {0},
                    nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            compressor
                .DecompressBlob(
                    {TRope(TString(data.Head(1))),
                     TRope(TString(data.SubStr(1)))},
                    outputs,
                    blob.Compression,
                    blob.Data.size(),
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
                    blob.Data.size(),
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
                    blob.Data.size(),
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
                    blob.Data.size(),
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
                    blob.Data.size(),
                    {8},
                    nullptr);
            });

        // The offset addresses an existing chunk, but lies beyond its partial
        // final payload. Chunk metadata alone cannot detect that in
        // PlanReadBlob.
        const auto partial = GetResultOrFail(
            compressor.CompressBlob(MakeSgList(TString(BlockSize, 'a'))));
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            compressor
                .DecompressBlob(
                    {TRope(partial.Data)},
                    outputs,
                    partial.Compression,
                    partial.Data.size(),
                    {1},
                    nullptr)
                .GetCode());
    }

    Y_UNIT_TEST(ShouldRejectInvalidCompressedChunksAndMetadata)
    {
        const TBlobsCompressor compressor(
            BlockSize,
            ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        const auto blob = GetResultOrFail(
            compressor.CompressBlob(MakeSgList(TString(ChunkSize, 'a'))));
        TString result(BlockSize, '?');
        const TSgList outputs = {{result.begin(), result.size()}};

        auto metadata = blob.Compression;
        metadata.MutablePlainChuncks()->AddChunkOffset(0);
        AssertArgumentError(
            [&]
            {
                return compressor.DecompressBlob(
                    {TRope(blob.Data)},
                    outputs,
                    metadata,
                    blob.Data.size(),
                    {0},
                    nullptr);
            });
        for (const TString codecName: {"", "unknown-codec"}) {
            metadata = blob.Compression;
            metadata.SetCodec(codecName);
            AssertArgumentError(
                [&]
                {
                    return compressor.DecompressBlob(
                        {TRope(blob.Data)},
                        outputs,
                        metadata,
                        blob.Data.size(),
                        {0},
                        nullptr);
                });
        }

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
                    blob.Data.size(),
                    {0},
                    nullptr)
                .GetCode());
        for (const ui64 size:
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
                        blob.Data.size(),
                        {0},
                        nullptr)
                    .GetCode());
        }

        // A valid length header without any compressed payload is corrupt.
        TString header(sizeof(ui64), '\0');
        WriteUnaligned<ui64>(header.begin(), ChunkSize);
        metadata = blob.Compression;
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            compressor
                .DecompressBlob(
                    {TRope(header)},
                    outputs,
                    metadata,
                    header.size(),
                    {0},
                    nullptr)
                .GetCode());

        // A truncated length header must also return a decoding error.
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            compressor
                .DecompressBlob(
                    {TRope(TString(header.data(), sizeof(ui64) - 1))},
                    outputs,
                    metadata,
                    sizeof(ui64) - 1,
                    {0},
                    nullptr)
                .GetCode());

        // Only the final chunk is allowed to be shorter than ChunkSize.
        const auto partial = GetResultOrFail(
            compressor.CompressBlob(MakeSgList(TString(BlockSize, 'a'))));
        metadata = partial.Compression;
        metadata.MutablePlainChuncks()->AddChunkOffset(partial.Data.size());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            compressor
                .DecompressBlob(
                    {TRope(partial.Data)},
                    outputs,
                    metadata,
                    2 * partial.Data.size(),
                    {0},
                    nullptr)
                .GetCode());
    }

    Y_UNIT_TEST(ShouldRejectInvalidChunkSizes)
    {
        for (const ui32 chunkSize: {BlockSize / 2, BlockSize + 1}) {
            AssertArgumentError(
                [&]
                {
                    return TBlobsCompressor(
                               BlockSize,
                               chunkSize,
                               "lz4-fast14-safe",
                               std::nullopt)
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
        NProto::TCompressionMetadata compression;
        compression.SetChunkSize(chunkSize);
        for (ui32 i = 1; i < (static_cast<ui32>(blockOffset) + 1) / 2; ++i) {
            compression.MutablePlainChuncks()->AddChunkOffset(
                i * compressedChunkSize);
        }

        const TBlobsCompressor compressor(
            blockSize,
            chunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        const auto [queries, queriesCount, blockOffsetsForEachRequest] =
            GetResultOrFail(compressor.PlanReadBlob(
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
        const TBlobsCompressor compressor(
            BlockSize,
            ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        const auto compression = MakeCompression();
        const NKikimr::TLogoBlobID blobId(TabletId, 1, 2, 3, 99, 4);

        // Chunk zero starts inside the blob but extends past its end; chunk
        // one starts outside the blob. Neither query can be sent to storage.
        for (const ui16 blockOffset: {0, 8}) {
            AssertArgumentError(
                [&]
                {
                    return compressor
                        .PlanReadBlob(blobId, compression, {blockOffset})
                        .GetError();
                });
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidInputs)
    {
        AssertArgumentError(
            [&]
            {
                return TBlobsCompressor(
                           0,
                           ChunkSize,
                           "lz4-fast14-safe",
                           std::nullopt)
                    .CompressBlob({})
                    .GetError();
            });
        AssertArgumentError(
            [&]
            {
                return TBlobsCompressor(
                           BlockSize,
                           0,
                           "lz4-fast14-safe",
                           std::nullopt)
                    .CompressBlob({})
                    .GetError();
            });
        for (const TStringBuf codecName: {"", "unknown-codec"}) {
            AssertArgumentError(
                [&]
                {
                    return TBlobsCompressor(
                               BlockSize,
                               ChunkSize,
                               codecName,
                               std::nullopt)
                        .CompressBlob({})
                        .GetError();
                });
        }

        const TBlobsCompressor compressor(
            BlockSize,
            ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        AssertArgumentError(
            [&]
            { return compressor.CompressBlob({{"unaligned", 9}}).GetError(); });
        AssertArgumentError(
            [&]
            {
                return compressor
                    .PlanReadBlob(MakeBlobId(), MakeCompression(), {40})
                    .GetError();
            });
        AssertArgumentError(
            [&] { return compressor.PlanReadBlob({}, {}, {0}).GetError(); });

        auto compression = MakeCompression();
        compression.MutablePlainChuncks()->SetChunkOffset(2, 0);
        AssertArgumentError(
            [&]
            {
                return compressor.PlanReadBlob(MakeBlobId(), compression, {16})
                    .GetError();
            });
    }

    Y_UNIT_TEST(ShouldReadSingleChunkWithoutOffsetsAndCalculateChecksums)
    {
        const TBlobsCompressor writer(
            BlockSize,
            ChunkSize,
            "lz4-fast14-safe",
            std::nullopt);
        const TString payload =
            TString(BlockSize, 'a') + TString(BlockSize, 'b');
        const auto blob =
            GetResultOrFail(writer.CompressBlob(MakeSgList(payload)));
        UNIT_ASSERT(blob.Compression.HasPlainChuncks());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            blob.Compression.GetPlainChuncks().ChunkOffsetSize());

        const TBlobsCompressor reader(
            BlockSize,
            "lz4-fast14-safe",
            std::nullopt);
        const TVector<ui16> blocks = {0, 1};
        const auto plan = GetResultOrFail(reader.PlanReadBlob(
            MakeBlobId(blob.Data.size()),
            blob.Compression,
            blocks));
        UNIT_ASSERT_VALUES_EQUAL(1, plan.QueriesCount);
        UNIT_ASSERT_VALUES_EQUAL(0, plan.Queries[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(blob.Data.size(), plan.Queries[0].Size);

        TString result(blocks.size() * BlockSize, '?');
        TVector<ui32> checksums;
        const auto error = reader.DecompressBlob(
            {TRope(blob.Data)},
            MakeSgList(result),
            blob.Compression,
            blob.Data.size(),
            blocks,
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

    Y_UNIT_TEST(ShouldRejectInvalidBoundaryOffsets)
    {
        const TBlobsCompressor reader(
            BlockSize,
            "lz4-fast14-safe",
            std::nullopt);
        TString output(BlockSize, '?');
        const TVector<TVector<ui32>> invalidOffsets =
            {{0}, {100, 100}, {200, 100}, {1500}, {1501}, {100, Max<ui32>()}};
        for (const auto& offsets: invalidOffsets) {
            NProto::TCompressionMetadata metadata;
            metadata.SetChunkSize(ChunkSize);
            for (const auto offset: offsets) {
                metadata.MutablePlainChuncks()->AddChunkOffset(offset);
            }
            AssertArgumentError(
                [&]
                {
                    return reader.PlanReadBlob(MakeBlobId(), metadata, {0})
                        .GetError();
                });
            AssertArgumentError(
                [&]
                {
                    return reader.DecompressBlob(
                        {},
                        MakeSgList(output),
                        metadata,
                        1500,
                        {0},
                        nullptr);
                });
        }

        NProto::TCompressionMetadata metadata;
        metadata.SetChunkSize(ChunkSize);
        // Missing representation must not be mistaken for a single chunk.
        AssertArgumentError(
            [&]
            {
                return reader.PlanReadBlob(MakeBlobId(), metadata, {0})
                    .GetError();
            });
        AssertArgumentError(
            [&]
            {
                return reader.DecompressBlob(
                    {},
                    MakeSgList(output),
                    metadata,
                    1500,
                    {0},
                    nullptr);
            });
        metadata.MutablePlainChuncks();
        AssertArgumentError(
            [&]
            {
                return reader.PlanReadBlob(MakeBlobId(0), metadata, {0})
                    .GetError();
            });
        AssertArgumentError(
            [&]
            {
                return reader.DecompressBlob(
                    {},
                    MakeSgList(output),
                    metadata,
                    0,
                    {0},
                    nullptr);
            });
        AssertArgumentError(
            [&]
            {
                return reader.DecompressBlob(
                    {},
                    {TBlockDataRef::CreateZeroBlock(BlockSize)},
                    metadata,
                    1500,
                    {0},
                    nullptr);
            });
    }

    Y_UNIT_TEST(ShouldRejectInvalidReaderSettings)
    {
        const auto compression = MakeCompression();
        const TBlobsCompressor reader(
            BlockSize,
            "lz4-fast14-safe",
            std::nullopt);
        AssertArgumentError([&] { return reader.CompressBlob({}).GetError(); });

        const TBlobsCompressor invalidReader(
            0,
            "lz4-fast14-safe",
            std::nullopt);
        AssertArgumentError(
            [&]
            {
                return invalidReader
                    .PlanReadBlob(MakeBlobId(), compression, {0})
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
                    1500,
                    {0},
                    &checksums);
            });
        UNIT_ASSERT_VALUES_EQUAL(TString(BlockSize, '?'), output);
        UNIT_ASSERT(checksums.empty());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
