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
#include <utility>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TCompression = NProto::TCompressionMetadata;

constexpr ui64 TabletId = 123456789;
constexpr ui32 BlockSize = 4096;
constexpr ui32 ChunkSize = 32 * 1024;
constexpr ui32 BlobHeaderSize = 5;

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

TRope MakeRope(std::initializer_list<TRope> buffers)
{
    TRope result;
    for (const auto& buffer: buffers) {
        result.Insert(result.End(), TRope(buffer));
    }
    return result;
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
        TRope(TString(data)),
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
    compression.SetCodec(NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE);
    compression.SetChunkSize(ChunkSize);
    compression.SetLastChunkSize(ChunkSize);
    for (const ui32 size: {100, 200, 300, 400}) {
        compression.MutablePlainChunks()->AddCompressedChunkSize(size);
    }
    return compression;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlobCompressorTest)
{
    Y_UNIT_TEST(ShouldKeepCompressedBlobsOnlyInSmallerHugeSlots)
    {
        const NKikimr::TBlobStorageGroupType groupType;
        const TString payload(ChunkSize + BlockSize, 'a');
        const auto blocks = MakeSgList(payload);
        const auto compressed = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            blocks));
        const ui32 compressedPartSize = compressed.Data.size() + BlobHeaderSize;
        const ui32 originalPartSize = payload.size() + BlobHeaderSize;
        UNIT_ASSERT(
            compressedPartSize > 1 && compressedPartSize < originalPartSize);

        struct TCase
        {
            TVector<ui32> Slots;
            bool Accepted;
        };

        const TCase cases[] = {
            {{}, false},
            {{0}, false},
            {{originalPartSize}, false},
            {{compressedPartSize, originalPartSize}, true},
            {{compressedPartSize - 1, originalPartSize}, false},
            {{compressedPartSize + 1, originalPartSize}, true},
            {{compressedPartSize, originalPartSize - 1}, false},
            {{compressedPartSize - 1}, false},
            {{originalPartSize, originalPartSize + 1}, false},
            {{originalPartSize + 1,
              compressedPartSize,
              originalPartSize,
              compressedPartSize,
              0},
             true},
            {{originalPartSize, 0, compressedPartSize - 1, originalPartSize},
             false}};
        for (const auto& [slots, accepted]: cases) {
            const TBlobCompressor compressor(
                NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                std::nullopt,
                TBlobCompressor::TCompressionConfig{
                    .BlockSize = BlockSize,
                    .ChunkSize = ChunkSize,
                    .HugeBlobSlotSizes = slots});
            const auto result = compressor.CompressBlobIfNeeded(
                blocks,
                true /* isSsd */,
                groupType);
            UNIT_ASSERT_VALUES_EQUAL(
                accepted ? S_OK : S_ALREADY,
                result.GetError().GetCode());
            if (accepted) {
                UNIT_ASSERT_VALUES_EQUAL(
                    compressed.Data,
                    result.GetResult().Data);
                UNIT_ASSERT_VALUES_EQUAL(
                    compressed.Compression.SerializeAsString(),
                    result.GetResult().Compression.SerializeAsString());
            }
        }
    }

    Y_UNIT_TEST(ShouldRejectCompressionThatIncreasesSlotSize)
    {
        const NKikimr::TBlobStorageGroupType groupType;
        TString payload(BlockSize, '\0');
        TReallyFastRng32 rng(42);
        for (auto& byte: payload) {
            byte = static_cast<char>(rng.GenRand());
        }
        const auto blocks = MakeSgList(payload);
        const auto compressed = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            blocks));
        UNIT_ASSERT(compressed.Data.size() > payload.size());
        const TBlobCompressor compressor(
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            std::nullopt,
            TBlobCompressor::TCompressionConfig{
                .BlockSize = BlockSize,
                .ChunkSize = ChunkSize,
                .HugeBlobSlotSizes = {
                    BlockSize + BlobHeaderSize,
                    static_cast<ui32>(compressed.Data.size()) +
                        BlobHeaderSize}});
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            compressor.CompressBlobIfNeeded(blocks, true /* isSsd */, groupType)
                .GetError()
                .GetCode());
    }

    Y_UNIT_TEST(ShouldUseTargetGroupErasureForSlotDecision)
    {
        using TErasure = NKikimr::TErasureType;

        struct TCase
        {
            TErasure::EErasureSpecies Erasure;
            ui32 PayloadSize;
            bool Accepted;
        };

        const TCase cases[] = {
            {TErasure::ErasureNone, 128, true},
            {TErasure::ErasureMirror3dc, 128, true},
            {TErasure::ErasureMirror3of4, 128, true},
            {TErasure::Erasure4Plus2Block, 128, false},
            {TErasure::Erasure4Plus2Block, 129, true},
            {TErasure::Erasure4Plus2Stripe, 128, false},
            {TErasure::Erasure4Plus2Stripe, 129, true}};
        const TBlobCompressor compressor(
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            std::nullopt,
            TBlobCompressor::TCompressionConfig{
                .BlockSize = 1,
                .ChunkSize = 256,
                .HugeBlobSlotSizes = {32, 64, 128, 256}});
        for (const auto& [erasure, size, accepted]: cases) {
            const TString payload(size, 'a');
            const NKikimr::TBlobStorageGroupType groupType(erasure);
            // With 4+2, 128 bytes and its compressed payload both need one
            // 32-byte part. The 129th input byte requires a second block.
            const auto result = compressor.CompressBlobIfNeeded(
                MakeSgList(payload, 1),
                true /* isSsd */,
                groupType);
            UNIT_ASSERT_VALUES_EQUAL_C(
                accepted ? S_OK : S_ALREADY,
                result.GetError().GetCode(),
                groupType.ToString() << ": " << size);
        }
    }

    Y_UNIT_TEST(ShouldAccountForEncodedPartCrc)
    {
        using TErasure = NKikimr::TErasureType;
        const NKikimr::TBlobStorageGroupType groupType(
            TErasure::Erasure4Plus2Block);
        const TBlobCompressor compressor(
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            std::nullopt,
            TBlobCompressor::TCompressionConfig{
                .BlockSize = 1,
                .ChunkSize = 256,
                .HugeBlobSlotSizes = {32 + BlobHeaderSize, 80, 256}});
        const TString payload(256, 'a');
        // Parts occupy 32 and 64 bytes plus the header without CRC. Adding a
        // four-byte CRC moves the compressed part into the original's slot.
        for (const auto crcMode:
             {TErasure::CrcModeNone, TErasure::CrcModeWholePart})
        {
            const auto result = compressor.CompressBlobIfNeeded(
                MakeSgList(payload, 1),
                true /* isSsd */,
                groupType,
                crcMode);
            UNIT_ASSERT_VALUES_EQUAL(
                crcMode == TErasure::CrcModeNone ? S_OK : S_ALREADY,
                result.GetError().GetCode());
        }
    }

    Y_UNIT_TEST(ShouldAccountForBlobHeaderInHugeSlotDecisions)
    {
        const NKikimr::TBlobStorageGroupType groupType(
            NKikimr::TErasureType::Erasure4Plus2Block);
        const TString payload(256, 'a');
        const auto blocks = MakeSgList(payload, 1);

        struct TCase
        {
            ui32 HeaderSize;
            bool Accepted;
        };

        const TCase cases[] = {
            {0, true},
            {BlobHeaderSize, true},
            {BlobHeaderSize + 1, false},
            {Max<ui32>(), false}};
        for (const auto& [headerSize, accepted]: cases) {
            const TBlobCompressor compressor(
                NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                std::nullopt,
                TBlobCompressor::TCompressionConfig{
                    .BlockSize = 1,
                    .ChunkSize = 256,
                    .HugeBlobSlotSizes = {32 + BlobHeaderSize, 80},
                    .HugeBlobHeaderSize = headerSize});
            // The compressed part fits the 37-byte slot exactly with a
            // five-byte header. A larger header moves it into the original's
            // slot. Oversized headers must not wrap part sizes around ui32.
            const auto result = compressor.CompressBlobIfNeeded(
                blocks,
                true /* isSsd */,
                groupType);
            UNIT_ASSERT_VALUES_EQUAL_C(
                accepted ? S_OK : S_ALREADY,
                result.GetError().GetCode(),
                headerSize);
        }
    }

    Y_UNIT_TEST(ShouldSelectHugeBlobPartSizeForMediaKind)
    {
        const NKikimr::TBlobStorageGroupType groupType(
            NKikimr::TErasureType::Erasure4Plus2Block);
        const TBlobCompressor compressor(
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            std::nullopt,
            TBlobCompressor::TCompressionConfig{
                .BlockSize = 1,
                .ChunkSize = 256,
                .HugeBlobSlotSizes = {32 + BlobHeaderSize, 64 + BlobHeaderSize},
                .MinHugeBlobPartSizeHDD = 128,
                .MinHugeBlobPartSizeSSD = 32 + BlobHeaderSize,
                .MinSavingsPercent = 100});
        const TString payload(256, 'a');
        const auto blocks = MakeSgList(payload, 1);
        // Encoded parts shrink from 64 to 32 bytes, plus the header. SSD
        // accepts the smaller slot; HDD treats the original as non-huge and
        // uses percent.
        for (const bool isSsd: {false, true}) {
            const auto result =
                compressor.CompressBlobIfNeeded(blocks, isSsd, groupType);
            UNIT_ASSERT_VALUES_EQUAL(
                isSsd ? S_OK : S_ALREADY,
                result.GetError().GetCode());
        }
    }

    Y_UNIT_TEST(ShouldRejectCompressionThatTurnsHugeBlobsIntoNonHugeBlobs)
    {
        using TErasure = NKikimr::TErasureType;
        const TString payload(256, 'a');
        const auto blocks = MakeSgList(payload, 1);
        const auto compressed = GetResultOrFail(CompressBlob(
            1,
            256,
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            blocks));
        for (const auto erasure:
             {TErasure::ErasureMirror3dc, TErasure::Erasure4Plus2Block})
        {
            const NKikimr::TBlobStorageGroupType groupType(erasure);
            for (const auto crcMode:
                 {TErasure::CrcModeNone, TErasure::CrcModeWholePart})
            {
                const ui32 originalPartSize =
                    groupType.MaxPartSize(crcMode, payload.size()) +
                    BlobHeaderSize;
                const ui32 compressedPartSize =
                    groupType.MaxPartSize(crcMode, compressed.Data.size()) +
                    BlobHeaderSize;
                UNIT_ASSERT(compressedPartSize > 1);
                UNIT_ASSERT(compressedPartSize < originalPartSize);

                struct TCase
                {
                    ui32 Threshold;
                    ui64 MinSavingsPercent;
                    bool Accepted;
                };

                const TCase cases[] = {
                    {0, 100, true},
                    {compressedPartSize - 1, 100, true},
                    {compressedPartSize, 100, true},
                    {compressedPartSize + 1, 0, false},
                    {originalPartSize, 0, false},
                    {originalPartSize + 1, 0, true},
                    {originalPartSize + 1, 100, false}};
                for (const auto& [threshold, minSavingsPercent, accepted]:
                     cases)
                {
                    const TBlobCompressor compressor(
                        NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                        std::nullopt,
                        TBlobCompressor::TCompressionConfig{
                            .BlockSize = 1,
                            .ChunkSize = 256,
                            .HugeBlobSlotSizes =
                                {compressedPartSize, originalPartSize},
                            .MinHugeBlobPartSizeSSD = threshold,
                            .MinSavingsPercent = minSavingsPercent});
                    const auto result = compressor.CompressBlobIfNeeded(
                        blocks,
                        true /* isSsd */,
                        groupType,
                        crcMode);
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        accepted ? S_OK : S_ALREADY,
                        result.GetError().GetCode(),
                        groupType.ToString()
                            << ": threshold=" << threshold
                            << ", crc=" << static_cast<ui32>(crcMode)
                            << ", minSavings=" << minSavingsPercent);
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldUseMinSavingsPercentForNonHugeBlobs)
    {
        const NKikimr::TBlobStorageGroupType groupType;
        const TString payload(100, 'a');
        const auto blocks = MakeSgList(payload, 1);
        const auto compressed = GetResultOrFail(CompressBlob(
            1,
            100,
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            blocks));
        UNIT_ASSERT(compressed.Data.size() < payload.size());
        const ui64 savingsPercent = payload.size() - compressed.Data.size();

        struct TCase
        {
            ui64 MinSavingsPercent;
            bool Accepted;
        };

        const TCase cases[] = {
            {0, true},
            {savingsPercent - 1, true},
            {savingsPercent, false},
            {savingsPercent + 1, false},
            {100, false},
            {Max<ui64>(), false}};
        for (const auto& [minSavingsPercent, accepted]: cases) {
            const TBlobCompressor compressor(
                NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                std::nullopt,
                TBlobCompressor::TCompressionConfig{
                    .BlockSize = 1,
                    .ChunkSize = 100,
                    .MinHugeBlobPartSizeSSD = 101 + BlobHeaderSize,
                    .MinSavingsPercent = minSavingsPercent});
            const auto result = compressor.CompressBlobIfNeeded(
                blocks,
                true /* isSsd */,
                groupType);
            UNIT_ASSERT_VALUES_EQUAL_C(
                accepted ? S_OK : S_ALREADY,
                result.GetError().GetCode(),
                minSavingsPercent);
            if (accepted) {
                UNIT_ASSERT_VALUES_EQUAL(
                    compressed.Data,
                    result.GetResult().Data);
            }
        }
    }

    Y_UNIT_TEST(ShouldValidateWriteCompressionGroupSettings)
    {
        using TErasure = NKikimr::TErasureType;
        const NKikimr::TBlobStorageGroupType groupType;
        const NKikimr::TBlobStorageGroupType invalidGroup(
            TErasure::ErasureSpeciesCount);
        const TBlobCompressor compressor(
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            std::nullopt,
            TBlobCompressor::TCompressionConfig{
                .BlockSize = BlockSize,
                .ChunkSize = ChunkSize,
                .HugeBlobSlotSizes = {BlockSize}});
        const TString payload(BlockSize, 'a');
        const auto blocks = MakeSgList(payload);
        AssertArgumentError(
            [&]
            {
                return compressor
                    .CompressBlobIfNeeded(
                        blocks,
                        true /* isSsd */,
                        invalidGroup)
                    .GetError();
            });
        AssertArgumentError(
            [&]
            {
                return compressor
                    .CompressBlobIfNeeded(
                        blocks,
                        true /* isSsd */,
                        groupType,
                        static_cast<TErasure::ECrcMode>(2))
                    .GetError();
            });

        const TBlobCompressor oversizedCompressor(
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            std::nullopt,
            TBlobCompressor::TCompressionConfig{
                .BlockSize = Max<ui32>(),
                .ChunkSize = Max<ui32>(),
                .HugeBlobSlotSizes = {Max<ui32>()}});
        const TSgList oversizedBlocks = {
            TBlockDataRef::CreateZeroBlock(Max<ui32>()),
            TBlockDataRef::CreateZeroBlock(Max<ui32>())};
        AssertArgumentError(
            [&]
            {
                return oversizedCompressor
                    .CompressBlobIfNeeded(
                        oversizedBlocks,
                        true /* isSsd */,
                        groupType)
                    .GetError();
            });
    }

    Y_UNIT_TEST(ShouldValidateWriteCompressionSettings)
    {
        const NKikimr::TBlobStorageGroupType groupType;
        const TString payload(BlockSize, 'a');
        for (const ui64 chunkSize: {ui64(BlockSize + 1), ui64(Max<ui32>()) + 1})
        {
            const TBlobCompressor compressor(
                NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                std::nullopt,
                TBlobCompressor::TCompressionConfig{
                    .BlockSize = BlockSize,
                    .ChunkSize = chunkSize,
                    .HugeBlobSlotSizes = {BlockSize}});
            AssertArgumentError(
                [&]
                {
                    return compressor
                        .CompressBlobIfNeeded(
                            MakeSgList(payload),
                            true /* isSsd */,
                            groupType)
                        .GetError();
                });
        }
        const TBlobCompressor estimator(
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            std::nullopt,
            std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            estimator
                .CompressBlobIfNeeded(
                    MakeSgList(payload),
                    true /* isSsd */,
                    groupType)
                .GetError()
                .GetCode());
    }

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
                UNIT_ASSERT(test.Type == blob.Compression.GetCodec());
                UNIT_ASSERT_VALUES_EQUAL(
                    NBlockCodecs::Codec(test.CodecName)->Encode(payload),
                    blob.Data);
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
        // The stored sizes total 1000 bytes, leaving no room for the last
        // chunk.
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
        compression.SetLastChunkSize(BlockSize);
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

        compression.MutablePlainChunks()->ClearCompressedChunkSize();
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
                config,
                std::nullopt);
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
                .PartCounters = counters},
            std::nullopt);

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
            const auto [compressed, compression, checksums] =
                GetResultOrFail(CompressBlob(
                    BlockSize,
                    chunkSize,
                    NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                    MakeSgList(payload)));

            UNIT_ASSERT(checksums.empty());
            UNIT_ASSERT(
                NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE ==
                compression.GetCodec());
            UNIT_ASSERT_VALUES_EQUAL(chunkSize, compression.GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(BlockSize, compression.GetLastChunkSize());
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
             {"lz4-fast14-safe", "snappy", "zstd_3"})
        {
            const auto codec =
                GetResultOrFail(ParseCompressionCodec(codecName));
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
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            MakeSgList(payload)));
        const TVector<ui16> blocks = {1, 2, 16};

        const auto plan = GetResultOrFail(PlanReadBlob(
            BlockSize,
            MakeBlobId(blob.Data.size()),
            blob.Compression,
            blocks));
        UNIT_ASSERT_VALUES_EQUAL(2, plan.Queries.size());
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

        TRope inputs;
        for (const auto& query: plan.Queries) {
            inputs.Insert(
                inputs.End(),
                TRope(TString(
                    TStringBuf(blob.Data).SubStr(query.Shift, query.Size))));
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
                NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                input));
            UNIT_ASSERT_VALUES_EQUAL(
                (blockCount + ChunkSize / BlockSize - 1) /
                        (ChunkSize / BlockSize) -
                    1,
                blob.Compression.GetPlainChunks().CompressedChunkSizeSize());
            UNIT_ASSERT_VALUES_EQUAL(
                ((blockCount - 1) % (ChunkSize / BlockSize) + 1) * BlockSize,
                blob.Compression.GetLastChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                payload,
                DecompressWholeBlob(
                    blob.Compression,
                    blob.Data,
                    payload.size()));
        }
    }

    Y_UNIT_TEST(ShouldReturnChecksumsOfCompressedBlocks)
    {
        constexpr ui32 blockCount = 2 * ChunkSize / BlockSize + 1;
        TVector<TString> blocks(blockCount);
        TSgList input;
        TString payload;
        for (ui32 i = 0; i < blockCount; ++i) {
            blocks[i] = TString(BlockSize, i % 3 ? 'a' + i : '\0');
            input.push_back(
                i % 3 ? TBlockDataRef(blocks[i].data(), BlockSize)
                      : TBlockDataRef::CreateZeroBlock(BlockSize));
            payload.append(blocks[i]);
        }

        const TBlobCompressor compressor(
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
            std::nullopt,
            TBlobCompressor::TCompressionConfig{
                .BlockSize = BlockSize,
                .ChunkSize = ChunkSize,
                .HugeBlobSlotSizes = {BlockSize, 3 * ChunkSize}});
        for (const bool calculateChecksums: {false, true}) {
            const auto blob = GetResultOrFail(compressor.CompressBlobIfNeeded(
                input,
                true /* isSsd */,
                NKikimr::TBlobStorageGroupType(),
                NKikimr::TErasureType::CrcModeNone,
                calculateChecksums));
            const auto restored = DecompressWholeBlob(
                blob.Compression,
                blob.Data,
                payload.size());
            UNIT_ASSERT_VALUES_EQUAL(payload, restored);
            UNIT_ASSERT_VALUES_EQUAL(
                calculateChecksums ? blockCount : 0,
                blob.BlockChecksums.size());
            for (size_t i = 0; i < blob.BlockChecksums.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    ComputeDefaultDigest(
                        {restored.data() + i * BlockSize, BlockSize}),
                    blob.BlockChecksums[i]);
            }
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

        const auto [compressed, compression, checksums] =
            GetResultOrFail(CompressBlob(
                BlockSize,
                ChunkSize,
                NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                MakeSgList(payload)));

        UNIT_ASSERT(checksums.empty());
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
                const auto [compressed, compression, checksums] =
                    GetResultOrFail(CompressBlob(
                        blockSize,
                        chunkSize,
                        NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                        MakeSgList(payload, blockSize)));
                UNIT_ASSERT(checksums.empty());
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
                MakeBlobId(blob.Data.size()),
                blob.Compression,
                blocks));
        UNIT_ASSERT_VALUES_EQUAL(2, queries.size());
        UNIT_ASSERT(queries[0].Shift > 0);
        TRope inputs;
        for (const auto& query: queries) {
            inputs.Insert(
                inputs.End(),
                TRope(TString(
                    TStringBuf(blob.Data).SubStr(query.Shift, query.Size))));
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
                NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                MakeSgList(payload)));
            NProto::TCompressionMetadata metadata;
            UNIT_ASSERT(
                metadata.ParseFromString(blob.Compression.SerializeAsString()));
            UNIT_ASSERT_VALUES_EQUAL(chunkSize, metadata.GetChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(BlockSize, metadata.GetLastChunkSize());
            UNIT_ASSERT_VALUES_EQUAL(
                3,
                metadata.GetPlainChunks().CompressedChunkSizeSize());

            const TVector<ui16> blocks = {
                static_cast<ui16>(blocksPerChunk),
                static_cast<ui16>(blocksPerChunk + 1),
                lastBlock};
            const auto [queries, blockRangesForEachRequest] =
                GetResultOrFail(PlanReadBlob(
                    BlockSize,
                    MakeBlobId(blob.Data.size()),
                    metadata,
                    blocks));
            UNIT_ASSERT_VALUES_EQUAL(2, queries.size());
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

            TRope inputs;
            for (const auto& query: queries) {
                inputs.Insert(
                    inputs.End(),
                    TRope(TString(TStringBuf(blob.Data).SubStr(
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
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
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
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
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
                               TVector<ui16>{0})
                        .GetError();
                });
            AssertArgumentError(
                [&]
                {
                    return DecompressBlob(
                        BlockSize,
                        TRope(blob.Data),
                        metadata,
                        blob.Data.size(),
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
            auto metadata = blob.Compression;
            metadata.SetLastChunkSize(size);
            AssertArgumentError(
                [&]
                {
                    return PlanReadBlob(
                               BlockSize,
                               MakeBlobId(blob.Data.size()),
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
                        blob.Data.size(),
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
                const auto blobId = MakeBlobId(blob.Data.size());
                GetResultOrFail(PlanReadBlob(
                    BlockSize,
                    blobId,
                    blob.Compression,
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
                                       blob.Compression,
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
                                blob.Compression,
                                blob.Data.size(),
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
            auto metadata = blob.Compression;
            metadata.SetLastChunkSize(size);
            TString output(BlockSize, '?');
            const auto error = DecompressBlob(
                BlockSize,
                TRope(blob.Data),
                metadata,
                blob.Data.size(),
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
        const auto data = TStringBuf(blob.Data);

        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    TRope(TString(data)),
                    blob.Compression,
                    blob.Data.size(),
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
                    blob.Compression,
                    blob.Data.size(),
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
                    blob.Compression,
                    blob.Data.size(),
                    TVector<ui16>{0},
                    TSgList{{result.begin(), BlockSize + 1}},
                    nullptr);
            });
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                {},
                blob.Compression,
                blob.Data.size(),
                TVector<ui16>{0},
                outputs,
                nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                MakeRope({{}, {}}),
                blob.Compression,
                blob.Data.size(),
                TVector<ui16>{0},
                outputs,
                nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                TRope(TString(data.Head(data.size() - 1))),
                blob.Compression,
                blob.Data.size(),
                TVector<ui16>{0},
                outputs,
                nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                MakeRope({TRope(TString(data)), TRope(TString("extra"))}),
                blob.Compression,
                blob.Data.size(),
                TVector<ui16>{0},
                outputs,
                nullptr)
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                TRope(TString(data)),
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
                    TRope(TString(data)),
                    blob.Compression,
                    blob.Data.size(),
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
                MakeRope(
                    {{},
                     TRope(TString(data.Head(1))),
                     {},
                     TRope(TString(data.SubStr(1, size - 1))),
                     {}}),
                blob.Compression,
                blob.Data.size(),
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
        WriteUnaligned<ui64>(blob.Data.begin(), ChunkSize);

        TString result(BlockSize, '?');
        const auto error = DecompressBlob(
            BlockSize,
            TRope(blob.Data),
            blob.Compression,
            blob.Data.size(),
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

        auto metadata = blob.Compression;
        metadata.MutablePlainChunks()->AddCompressedChunkSize(0);
        AssertArgumentError(
            [&]
            {
                return DecompressBlob(
                    BlockSize,
                    TRope(blob.Data),
                    metadata,
                    blob.Data.size(),
                    TVector<ui16>{0},
                    outputs,
                    nullptr);
            });
        for (const int value:
             {-1, 0, NCloud::NProto::ECompressionCodec_MAX + 1, Max<int>()})
        {
            const auto codec =
                static_cast<NCloud::NProto::ECompressionCodec>(value);
            metadata = blob.Compression;
            metadata.SetCodec(codec);
            AssertArgumentError(
                [&]
                {
                    return PlanReadBlob(
                               BlockSize,
                               MakeBlobId(blob.Data.size()),
                               metadata,
                               TVector<ui16>{0})
                        .GetError();
                });
            AssertArgumentError(
                [&]
                {
                    return DecompressBlob(
                        BlockSize,
                        TRope(blob.Data),
                        metadata,
                        blob.Data.size(),
                        TVector<ui16>{0},
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
                TRope(emptyChunk),
                blob.Compression,
                blob.Data.size(),
                TVector<ui16>{0},
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
                    TRope(corrupt),
                    blob.Compression,
                    blob.Data.size(),
                    TVector<ui16>{0},
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
                TRope(header),
                metadata,
                header.size(),
                TVector<ui16>{0},
                outputs,
                nullptr)
                .GetCode());

        // A truncated length header must also return a decoding error.
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
        metadata = partial.Compression;
        metadata.MutablePlainChunks()->AddCompressedChunkSize(
            partial.Data.size());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            DecompressBlob(
                BlockSize,
                TRope(partial.Data),
                metadata,
                2 * partial.Data.size(),
                TVector<ui16>{0},
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
                               NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
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
        compression.SetCodec(NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE);
        compression.SetChunkSize(chunkSize);
        compression.SetLastChunkSize(chunkSize);
        for (ui32 i = 1; i < (static_cast<ui32>(blockOffset) + 1) / 2; ++i) {
            compression.MutablePlainChunks()->AddCompressedChunkSize(
                compressedChunkSize);
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
        AssertArgumentError(
            [&]
            {
                return CompressBlob(
                           BlockSize,
                           0,
                           NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
                           {})
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
        compression.MutablePlainChunks()->SetCompressedChunkSize(2, 0);
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

    Y_UNIT_TEST(ShouldReadSingleChunkWithoutStoredSizesAndCalculateChecksums)
    {
        const TString payload =
            TString(BlockSize, 'a') + TString(BlockSize, 'b');
        const auto blob = GetResultOrFail(CompressBlob(
            BlockSize,
            ChunkSize,
            NCloud::NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
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
        UNIT_ASSERT_VALUES_EQUAL(1, plan.Queries.size());
        UNIT_ASSERT_VALUES_EQUAL(0, plan.Queries[0].Shift);
        UNIT_ASSERT_VALUES_EQUAL(blob.Data.size(), plan.Queries[0].Size);

        TString result(blocks.size() * BlockSize, '?');
        TVector<ui32> checksums;
        const auto error = DecompressBlob(
            BlockSize,
            TRope(blob.Data),
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
            metadata.SetLastChunkSize(ChunkSize);
            for (const auto size: sizes) {
                metadata.MutablePlainChunks()->AddCompressedChunkSize(size);
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
        NProto::TCompressionMetadata metadata;
        metadata.SetChunkSize(ChunkSize);
        metadata.SetLastChunkSize(ChunkSize);
        // Missing representation must not be mistaken for a single chunk.
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
        metadata.MutablePlainChunks();
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
