#include "blob_compression.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/random/fast.h>
#include <util/string/printf.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 BlockSize = 4_KB;

constexpr auto Lz4 = NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE;

TString CompressibleData(size_t size)
{
    TString data;
    data.reserve(size);
    ui32 i = 0;
    while (data.size() < size) {
        data += Sprintf("line %010u: the quick brown fox\n", i++);
    }
    data.resize(size);
    return data;
}

TString RandomData(size_t size)
{
    TReallyFastRng32 rng(12345);
    TString data;
    data.ReserveAndResize(size);
    for (auto& c: data) {
        c = static_cast<char>(rng.GenRand());
    }
    return data;
}

TCompressedBlob CompressOrFail(
    NProto::ECompressionCodec codec,
    ui32 chunkSize,
    ui32 minSavedPercent,
    const TString& data,
    const TVector<ui32>& chunkSizes)
{
    auto result = CompressChunks(
        codec,
        BlockSize,
        chunkSize,
        minSavedPercent,
        data,
        chunkSizes);
    UNIT_ASSERT_C(!HasError(result.GetError()), FormatError(result.GetError()));
    return result.ExtractResult();
}

TBlobChunkLayout LayoutOrFail(const TCompressedBlob& blob)
{
    NProto::TBlobCompressionInfo info;
    UNIT_ASSERT(info.ParseFromString(blob.Info.SerializeAsString()));
    auto layout = TBlobChunkLayout::Create(info, blob.Content.size(), BlockSize);
    UNIT_ASSERT_C(!HasError(layout.GetError()), FormatError(layout.GetError()));
    return layout.ExtractResult();
}

TString DecompressOrFail(
    const TCompressedBlob& blob,
    ui32 firstChunk,
    ui32 lastChunk)
{
    const auto layout = LayoutOrFail(blob);

    TString out;
    out.ReserveAndResize(layout.GetSpanLength(firstChunk, lastChunk));
    const auto error = DecompressChunks(
        layout,
        TStringBuf(blob.Content).SubStr(
            layout.GetStoredChunkOffset(firstChunk),
            layout.GetStoredSpanLength(firstChunk, lastChunk)),
        firstChunk,
        lastChunk,
        TArrayRef<char>(out.begin(), out.size()));
    UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());
    return out;
}

ui32 ValidationCode(const NProto::TBlobCompressionInfo& info, ui64 blobSize)
{
    return TBlobChunkLayout::Create(info, blobSize, BlockSize)
        .GetError()
        .GetCode();
}

ui32 CompressionCode(
    ui32 blockSize,
    ui32 chunkSize,
    const TString& data,
    const TVector<ui32>& chunkSizes)
{
    return CompressChunks(Lz4, blockSize, chunkSize, 12, data, chunkSizes)
        .GetError()
        .GetCode();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlobCompressionTest)
{
    Y_UNIT_TEST(ShouldCheckWhetherCompressionIsWorthIt)
    {
        UNIT_ASSERT(IsWorthStoringCompressed(88, 100, 12));
        UNIT_ASSERT(!IsWorthStoringCompressed(89, 100, 12));
        UNIT_ASSERT(IsWorthStoringCompressed(97, 100, 3));
        UNIT_ASSERT(!IsWorthStoringCompressed(98, 100, 3));
        UNIT_ASSERT(IsWorthStoringCompressed(99, 100, 1));
        UNIT_ASSERT(!IsWorthStoringCompressed(100, 100, 1));
        UNIT_ASSERT(!IsWorthStoringCompressed(101, 100, 1));
        UNIT_ASSERT(IsWorthStoringCompressed(0, 100, 100));
        UNIT_ASSERT(!IsWorthStoringCompressed(1, 100, 100));

        UNIT_ASSERT(IsWorthStoringCompressed(99, 100, 0));
        UNIT_ASSERT(!IsWorthStoringCompressed(100, 100, 0));
        UNIT_ASSERT(!IsWorthStoringCompressed(101, 100, 0));

        UNIT_ASSERT(!IsWorthStoringCompressed(0, 100, 101));
        UNIT_ASSERT(!IsWorthStoringCompressed(0, 100, Max<ui32>()));
    }

    Y_UNIT_TEST(ShouldSplitIntoChunks)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui32>(),
            SplitIntoChunks(TVector<ui32>(), 16_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui32>{16_KB, 16_KB, 8_KB}),
            SplitIntoChunks(TVector<ui32>{40_KB}, 16_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui32>{12_KB, 16_KB, 16_KB, 4_KB, 16_KB}),
            SplitIntoChunks(TVector<ui32>{12_KB, 32_KB, 0, 4_KB, 16_KB}, 16_KB));
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<ui32>(),
            SplitIntoChunks(TVector<ui32>{40_KB}, 0));
    }

    Y_UNIT_TEST(ShouldCompressAndDecompressFullChunks)
    {
        const auto data = CompressibleData(1_MB + 24_KB);
        const auto chunkSizes =
            SplitIntoChunks(TVector<ui32>{1_MB + 24_KB}, 64_KB);
        const auto blob = CompressOrFail(Lz4, 64_KB, 12, data, chunkSizes);

        UNIT_ASSERT(IsCompressed(blob.Info));
        UNIT_ASSERT(Lz4 == blob.Info.GetCodec());
        UNIT_ASSERT(blob.Info.HasRegularChunks());
        UNIT_ASSERT_VALUES_EQUAL(
            64_KB,
            blob.Info.GetRegularChunks().GetMaxUncompressedChunkSize());
        UNIT_ASSERT_VALUES_EQUAL(17, blob.Info.CompressedChunkSizesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            24_KB,
            blob.Info.GetRegularChunks().GetLastUncompressedChunkSize());
        UNIT_ASSERT(blob.Content.size() < data.size() / 2);

        const auto layout = LayoutOrFail(blob);
        UNIT_ASSERT(GetCompressionCodec(Lz4).GetResult() == layout.GetCodec());
        UNIT_ASSERT_VALUES_EQUAL(17, layout.GetChunkCount());
        UNIT_ASSERT_VALUES_EQUAL(1_MB + 24_KB, layout.GetUncompressedSize());
        for (ui32 chunk = 0; chunk < layout.GetChunkCount(); ++chunk) {
            UNIT_ASSERT(!layout.IsRawChunk(chunk));
            UNIT_ASSERT_VALUES_EQUAL(
                chunk * 64_KB,
                layout.GetChunkOffset(chunk));
            UNIT_ASSERT_VALUES_EQUAL(
                chunk == 16 ? 24_KB : 64_KB,
                layout.GetChunkLength(chunk));
        }
        UNIT_ASSERT_VALUES_EQUAL(16, layout.GetChunkIndex(1_MB));
        UNIT_ASSERT_VALUES_EQUAL(16, layout.GetChunkIndex(1_MB + 24_KB - 1));
        UNIT_ASSERT_VALUES_EQUAL(15, layout.GetChunkIndex(1_MB - 1));

        UNIT_ASSERT_VALUES_EQUAL(data, DecompressOrFail(blob, 0, 16));
        UNIT_ASSERT_VALUES_EQUAL(
            data.substr(128_KB, 192_KB),
            DecompressOrFail(blob, 2, 4));
        UNIT_ASSERT_VALUES_EQUAL(
            data.substr(1_MB, 24_KB),
            DecompressOrFail(blob, 16, 16));

        // a single chunk is the last one
        const auto small = CompressOrFail(
            Lz4,
            64_KB,
            12,
            data.substr(0, 8_KB),
            TVector<ui32>{8_KB});
        UNIT_ASSERT(small.Info.HasRegularChunks());
        UNIT_ASSERT_VALUES_EQUAL(
            8_KB,
            small.Info.GetRegularChunks().GetLastUncompressedChunkSize());
        UNIT_ASSERT_VALUES_EQUAL(
            data.substr(0, 8_KB),
            DecompressOrFail(small, 0, 0));
    }

    Y_UNIT_TEST(ShouldCompressChunksOfDifferentSizes)
    {
        const auto data = CompressibleData(108_KB);
        const auto chunkSizes =
            SplitIntoChunks(TVector<ui32>{12_KB, 40_KB, 4_KB, 52_KB}, 16_KB);
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui32>{
                12_KB,
                16_KB, 16_KB, 8_KB,
                4_KB,
                16_KB, 16_KB, 16_KB, 4_KB}),
            chunkSizes);

        const auto blob = CompressOrFail(Lz4, 16_KB, 12, data, chunkSizes);
        UNIT_ASSERT(blob.Info.HasExplicitChunks());
        UNIT_ASSERT_VALUES_EQUAL(
            9,
            blob.Info.GetExplicitChunks().UncompressedChunkSizesSize());

        const auto layout = LayoutOrFail(blob);
        UNIT_ASSERT_VALUES_EQUAL(9, layout.GetChunkCount());
        UNIT_ASSERT_VALUES_EQUAL(108_KB, layout.GetUncompressedSize());

        ui64 offset = 0;
        ui64 storedOffset = 0;
        for (ui32 chunk = 0; chunk < layout.GetChunkCount(); ++chunk) {
            UNIT_ASSERT_VALUES_EQUAL(offset, layout.GetChunkOffset(chunk));
            UNIT_ASSERT_VALUES_EQUAL(
                chunkSizes[chunk],
                layout.GetChunkLength(chunk));
            UNIT_ASSERT_VALUES_EQUAL(
                storedOffset,
                layout.GetStoredChunkOffset(chunk));
            UNIT_ASSERT_VALUES_EQUAL(
                blob.Info.GetCompressedChunkSizes(chunk),
                layout.GetStoredChunkLength(chunk));

            UNIT_ASSERT_VALUES_EQUAL(chunk, layout.GetChunkIndex(offset));
            UNIT_ASSERT_VALUES_EQUAL(
                chunk,
                layout.GetChunkIndex(offset + chunkSizes[chunk] - 1));

            UNIT_ASSERT_VALUES_EQUAL(
                data.substr(offset, chunkSizes[chunk]),
                DecompressOrFail(blob, chunk, chunk));

            offset += chunkSizes[chunk];
            storedOffset += blob.Info.GetCompressedChunkSizes(chunk);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            blob.Content.size(),
            layout.GetStoredSpanLength(0, 8));
        UNIT_ASSERT_VALUES_EQUAL(
            blob.Info.GetCompressedChunkSizes(3)
                + blob.Info.GetCompressedChunkSizes(4),
            layout.GetStoredSpanLength(3, 4));

        UNIT_ASSERT_VALUES_EQUAL(108_KB, layout.GetSpanLength(0, 8));
        UNIT_ASSERT_VALUES_EQUAL(12_KB, layout.GetSpanLength(3, 4));

        UNIT_ASSERT_VALUES_EQUAL(data, DecompressOrFail(blob, 0, 8));
        UNIT_ASSERT_VALUES_EQUAL(
            data.substr(44_KB, 28_KB),
            DecompressOrFail(blob, 3, 5));
    }

    Y_UNIT_TEST(ShouldKeepIncompressibleChunksRaw)
    {
        const auto data = CompressibleData(64_KB) + RandomData(32_KB)
            + CompressibleData(32_KB);
        const auto chunkSizes =
            SplitIntoChunks(TVector<ui32>{64_KB, 32_KB, 32_KB}, 16_KB);

        const auto blob = CompressOrFail(Lz4, 16_KB, 12, data, chunkSizes);

        const auto layout = LayoutOrFail(blob);
        UNIT_ASSERT_VALUES_EQUAL(8, layout.GetChunkCount());
        for (ui32 chunk = 0; chunk < layout.GetChunkCount(); ++chunk) {
            const bool raw = chunk == 4 || chunk == 5;
            UNIT_ASSERT_VALUES_EQUAL_C(raw, layout.IsRawChunk(chunk), chunk);
            if (raw) {
                UNIT_ASSERT_VALUES_EQUAL(
                    0,
                    blob.Info.GetCompressedChunkSizes(chunk));
                UNIT_ASSERT_VALUES_EQUAL(
                    16_KB,
                    layout.GetStoredChunkLength(chunk));
                UNIT_ASSERT_VALUES_EQUAL(
                    data.substr(layout.GetChunkOffset(chunk), 16_KB),
                    blob.Content.substr(
                        layout.GetStoredChunkOffset(chunk),
                        16_KB));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    blob.Info.GetCompressedChunkSizes(chunk),
                    layout.GetStoredChunkLength(chunk));
                UNIT_ASSERT(layout.GetStoredChunkLength(chunk) < 16_KB);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(
            blob.Content.size(),
            layout.GetStoredSpanLength(0, 7));

        UNIT_ASSERT_VALUES_EQUAL(data, DecompressOrFail(blob, 0, 7));
        UNIT_ASSERT_VALUES_EQUAL(
            data.substr(48_KB, 48_KB),
            DecompressOrFail(blob, 3, 5));
    }

    Y_UNIT_TEST(ShouldNotStoreChunkCompressedIfItDoesNotShrink)
    {
        const auto data = RandomData(64_KB);
        const auto chunkSizes = SplitIntoChunks(TVector<ui32>{64_KB}, 16_KB);

        for (const ui32 minSavedPercent: {0, 12, 100, 101}) {
            const auto blob =
                CompressOrFail(Lz4, 16_KB, minSavedPercent, data, chunkSizes);
            UNIT_ASSERT_VALUES_EQUAL(data, blob.Content);

            const auto layout = LayoutOrFail(blob);
            for (ui32 chunk = 0; chunk < layout.GetChunkCount(); ++chunk) {
                UNIT_ASSERT(layout.IsRawChunk(chunk));
            }
            UNIT_ASSERT_VALUES_EQUAL(data, DecompressOrFail(blob, 0, 3));
        }
    }

    Y_UNIT_TEST(ShouldNotStoreChunkCompressedIfItHasTheSameSize)
    {
        const auto* codec = GetCompressionCodec(Lz4).GetResult();
        const TVector<ui32> chunkSizes = {16_KB};

        // every extra byte of the run makes the compressed chunk smaller, at
        // some length it has exactly the size of its data
        bool sameSize = false;
        for (ui32 run = 0; run < 320; ++run) {
            const auto data = RandomData(16_KB - run) + TString(run, 'a');
            const auto blob = CompressOrFail(Lz4, 16_KB, 0, data, chunkSizes);
            UNIT_ASSERT_VALUES_EQUAL_C(data, DecompressOrFail(blob, 0, 0), run);

            if (codec->Encode(data).size() == data.size()) {
                sameSize = true;
                UNIT_ASSERT_VALUES_EQUAL_C(data, blob.Content, run);
            }
        }
        UNIT_ASSERT(sameSize);
    }

    Y_UNIT_TEST(ShouldApplyThresholdToSmallSavings)
    {
        // a run of equal bytes at the end of a random chunk compresses to a
        // few bytes: the chunk saves about 1.5% and about 6% of its size
        const auto small = RandomData(16_KB - 320) + TString(320, 'a');
        const auto large = RandomData(16_KB - 1100) + TString(1100, 'a');
        const TVector<ui32> chunkSizes = {16_KB};

        const auto isRaw = [&] (ui32 minSavedPercent, const TString& data) {
            const auto blob =
                CompressOrFail(Lz4, 16_KB, minSavedPercent, data, chunkSizes);
            UNIT_ASSERT_VALUES_EQUAL(data, DecompressOrFail(blob, 0, 0));
            return LayoutOrFail(blob).IsRawChunk(0);
        };

        UNIT_ASSERT(!isRaw(0, small));
        UNIT_ASSERT(!isRaw(0, large));
        UNIT_ASSERT(isRaw(3, small));
        UNIT_ASSERT(!isRaw(3, large));
        UNIT_ASSERT(isRaw(12, small));
        UNIT_ASSERT(isRaw(12, large));
    }

    Y_UNIT_TEST(ShouldApplyThresholdToChunks)
    {
        // the first half of every chunk is compressible, the second is not:
        // every chunk saves about 30%
        TString data;
        for (ui32 i = 0; i < 4; ++i) {
            data += CompressibleData(8_KB) + RandomData(8_KB);
        }
        const auto chunkSizes = SplitIntoChunks(TVector<ui32>{64_KB}, 16_KB);

        auto blob = CompressOrFail(Lz4, 16_KB, 12, data, chunkSizes);
        for (ui32 chunk = 0; chunk < 4; ++chunk) {
            UNIT_ASSERT(!LayoutOrFail(blob).IsRawChunk(chunk));
        }
        UNIT_ASSERT(blob.Content.size() < 56_KB);
        UNIT_ASSERT_VALUES_EQUAL(data, DecompressOrFail(blob, 0, 3));

        blob = CompressOrFail(Lz4, 16_KB, 50, data, chunkSizes);
        for (ui32 chunk = 0; chunk < 4; ++chunk) {
            UNIT_ASSERT(LayoutOrFail(blob).IsRawChunk(chunk));
        }
        UNIT_ASSERT_VALUES_EQUAL(data, blob.Content);
    }

    Y_UNIT_TEST(ShouldCompressWithEveryCodec)
    {
        const auto data = CompressibleData(64_KB);
        const auto chunkSizes = SplitIntoChunks(TVector<ui32>{64_KB}, 16_KB);

        for (int value = NProto::ECompressionCodec_MIN;
             value <= NProto::ECompressionCodec_MAX;
             ++value)
        {
            const auto codec = static_cast<NProto::ECompressionCodec>(value);
            if (codec == NProto::COMPRESSION_CODEC_NONE) {
                continue;
            }

            const auto blob = CompressOrFail(codec, 16_KB, 12, data, chunkSizes);
            UNIT_ASSERT_C(codec == blob.Info.GetCodec(), value);
            UNIT_ASSERT(blob.Content.size() < data.size());
            UNIT_ASSERT_VALUES_EQUAL_C(
                data,
                DecompressOrFail(blob, 0, 3),
                value);
        }
    }

    Y_UNIT_TEST(ShouldRejectInvalidCompressionArguments)
    {
        const auto data = CompressibleData(32_KB);

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CompressChunks(
                NProto::COMPRESSION_CODEC_NONE,
                BlockSize,
                16_KB,
                12,
                data,
                TVector<ui32>{16_KB, 16_KB}).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CompressionCode(BlockSize, 16_KB, data, {}));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CompressionCode(BlockSize, 16_KB, data, {16_KB, 0, 16_KB}));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CompressionCode(BlockSize, 16_KB, data, {32_KB}));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CompressionCode(BlockSize, 16_KB, data, {16_KB, 8_KB}));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CompressionCode(BlockSize, 16_KB, data, {16_KB, 16_KB, 4_KB}));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CompressionCode(BlockSize, 16_KB, data, {16_KB, 16_KB}));

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CompressionCode(0, 16_KB, data, {16_KB, 16_KB}));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CompressionCode(BlockSize, 0, data, {16_KB, 16_KB}));

        // 6_KB is a multiple of a 2_KB block, but not of a 4_KB one
        const TVector<ui32> smallChunks(8, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CompressionCode(BlockSize, 6_KB, data, smallChunks));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CompressionCode(2_KB, 6_KB, data, smallChunks));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CompressionCode(BlockSize, 16_KB, data, {16_KB, 6_KB, 10_KB}));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CompressionCode(2_KB, 16_KB, data, {16_KB, 6_KB, 10_KB}));
    }

    Y_UNIT_TEST(ShouldReportCodecFailures)
    {
        // lz4 accepts at most 2 GiB and fails before reading the data
        const auto data = CompressibleData(16_KB);
        const TStringBuf tooLarge(data.data(), 3_GB);

        const auto result = CompressChunks(
            Lz4,
            BlockSize,
            16_KB,
            12,
            tooLarge,
            TVector<ui32>{16_KB});
        UNIT_ASSERT_VALUES_EQUAL(E_FAIL, result.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldValidateCompressionInfo)
    {
        // chunks of 16_KB, 8_KB and 16_KB: the sizes are stored explicitly
        const auto data = CompressibleData(40_KB);
        const auto blob = CompressOrFail(
            Lz4,
            16_KB,
            12,
            data,
            SplitIntoChunks(TVector<ui32>{24_KB, 16_KB}, 16_KB));
        const ui64 blobSize = blob.Content.size();
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            blob.Info.GetExplicitChunks().UncompressedChunkSizesSize());

        UNIT_ASSERT(!IsCompressed(NProto::TBlobCompressionInfo()));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, ValidationCode(blob.Info, blobSize));

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            ValidationCode(NProto::TBlobCompressionInfo(), 0));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            ValidationCode(blob.Info, blobSize + 1));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            TBlobChunkLayout::Create(blob.Info, blobSize, 0)
                .GetError()
                .GetCode());
        // the 8_KB chunk is not a multiple of a 16_KB block
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            TBlobChunkLayout::Create(blob.Info, blobSize, 16_KB)
                .GetError()
                .GetCode());

        auto info = blob.Info;
        info.SetCodec(NProto::COMPRESSION_CODEC_NONE);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ValidationCode(info, blobSize));

        info = blob.Info;
        info.SetCodec(static_cast<NProto::ECompressionCodec>(1000));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ValidationCode(info, blobSize));

        info = blob.Info;
        info.ClearExplicitChunks();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ValidationCode(info, blobSize));

        info = blob.Info;
        info.MutableExplicitChunks()->ClearUncompressedChunkSizes();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ValidationCode(info, blobSize));

        info = blob.Info;
        info.MutableExplicitChunks()->AddUncompressedChunkSizes(4_KB);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ValidationCode(info, blobSize));

        info = blob.Info;
        info.ClearCompressedChunkSizes();
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ValidationCode(info, blobSize));

        info = blob.Info;
        info.MutableExplicitChunks()->SetUncompressedChunkSizes(1, 0);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ValidationCode(info, blobSize));

        info = blob.Info;
        info.MutableExplicitChunks()->SetUncompressedChunkSizes(1, 8_KB - 1);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ValidationCode(info, blobSize));

        // a raw chunk occupies its uncompressed size
        const ui64 otherChunks =
            blobSize - blob.Info.GetCompressedChunkSizes(1);
        info = blob.Info;
        info.SetCompressedChunkSizes(1, 0);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ValidationCode(info, blobSize));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            ValidationCode(info, otherChunks + 8_KB));

        // a compressed chunk of the same size as its data is not raw
        info.SetCompressedChunkSizes(1, 8_KB);
        const auto sameSize =
            TBlobChunkLayout::Create(info, otherChunks + 8_KB, BlockSize);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, sameSize.GetError().GetCode());
        UNIT_ASSERT(!sameSize.GetResult().IsRawChunk(1));

        // a chunk is stored larger than its data
        info.SetCompressedChunkSizes(1, 8_KB + 1);
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            ValidationCode(info, otherChunks + 8_KB + 1));

        // The regular layout stores the maximum and last chunk sizes.
        const auto full = CompressOrFail(
            Lz4,
            16_KB,
            12,
            data,
            SplitIntoChunks(TVector<ui32>{40_KB}, 16_KB));
        const ui64 fullSize = full.Content.size();
        UNIT_ASSERT(full.Info.HasRegularChunks());
        UNIT_ASSERT_VALUES_EQUAL(
            8_KB,
            full.Info.GetRegularChunks().GetLastUncompressedChunkSize());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, ValidationCode(full.Info, fullSize));

        info = full.Info;
        info.MutableRegularChunks()->SetMaxUncompressedChunkSize(0);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ValidationCode(info, fullSize));

        info = full.Info;
        info.MutableRegularChunks()->SetMaxUncompressedChunkSize(16_KB + 1);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ValidationCode(info, fullSize));

        info = full.Info;
        info.MutableRegularChunks()->SetLastUncompressedChunkSize(0);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ValidationCode(info, fullSize));

        info = full.Info;
        info.MutableRegularChunks()->SetLastUncompressedChunkSize(8_KB + 1);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ValidationCode(info, fullSize));

        info = full.Info;
        info.MutableRegularChunks()->SetLastUncompressedChunkSize(32_KB);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ValidationCode(info, fullSize));

        // the last chunk is stored larger than LastUncompressedChunkSize
        info = full.Info;
        info.MutableRegularChunks()->SetLastUncompressedChunkSize(4_KB);
        info.SetCompressedChunkSizes(2, 4_KB + 1);
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            ValidationCode(
                info,
                fullSize + 4_KB + 1 - full.Info.GetCompressedChunkSizes(2)));
    }

    Y_UNIT_TEST(ShouldReportDecompressionErrors)
    {
        const auto data = CompressibleData(40_KB) + RandomData(16_KB);
        const auto blob = CompressOrFail(
            Lz4,
            16_KB,
            12,
            data,
            SplitIntoChunks(TVector<ui32>{40_KB, 16_KB}, 16_KB));
        const auto layout = LayoutOrFail(blob);
        UNIT_ASSERT_VALUES_EQUAL(4, layout.GetChunkCount());
        UNIT_ASSERT(layout.IsRawChunk(3));

        TString buffer;
        buffer.ReserveAndResize(layout.GetSpanLength(0, 3));
        const TArrayRef<char> out(buffer.begin(), buffer.size());

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            DecompressChunks(layout, blob.Content, 2, 1, out).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            DecompressChunks(layout, blob.Content, 0, 4, out).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            DecompressChunks(
                layout,
                blob.Content,
                0,
                2,
                out.first(layout.GetSpanLength(0, 2))).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            DecompressChunks(
                layout,
                blob.Content,
                0,
                3,
                out.first(out.size() - 1)).GetCode());

        auto corrupted = blob.Content;
        for (size_t i = 16; i < layout.GetStoredChunkOffset(3); i += 7) {
            corrupted[i] = ~corrupted[i];
        }
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            DecompressChunks(layout, corrupted, 0, 3, out).GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            DecompressChunks(layout, blob.Content, 0, 3, out).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(data, buffer);

        const auto chunk = [&] (ui32 i) {
            return TStringBuf(blob.Content).SubStr(
                layout.GetStoredChunkOffset(i),
                layout.GetStoredChunkLength(i));
        };

        const auto chunkOut = out.first(16_KB);
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            DecompressChunk(layout, 4, chunk(1), chunkOut).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            DecompressChunk(
                layout,
                1,
                chunk(1).Head(chunk(1).size() - 1),
                chunkOut).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            DecompressChunk(layout, 1, chunk(1), out.first(16_KB - 1))
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            DecompressChunk(layout, 1, chunk(1), chunkOut).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            data.substr(16_KB, 16_KB),
            TStringBuf(chunkOut.data(), chunkOut.size()));

        // chunk 2 is 8_KB: a frame which declares another length is rejected
        auto wrong = blob;
        wrong.Info.SetCompressedChunkSizes(
            2,
            blob.Info.GetCompressedChunkSizes(1));
        wrong.Content = TString::Join(chunk(0), chunk(1), chunk(1), chunk(3));
        const auto wrongLayout = LayoutOrFail(wrong);
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            DecompressChunk(wrongLayout, 2, chunk(1), out.first(8_KB))
                .GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            DecompressChunks(wrongLayout, wrong.Content, 0, 3, out).GetCode());

        // a layout which failed to build has no chunks
        const auto failed = TBlobChunkLayout::Create(
            blob.Info,
            blob.Content.size() + 1,
            BlockSize);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, failed.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(0, failed.GetResult().GetChunkCount());
        UNIT_ASSERT_VALUES_EQUAL(0, failed.GetResult().GetUncompressedSize());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            DecompressChunk(failed.GetResult(), 0, chunk(0), chunkOut)
                .GetCode());
    }
}

}   // namespace NCloud
