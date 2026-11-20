#include "codec.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T GetResultOrFail(TResultOrError<T> result)
{
    UNIT_ASSERT_C(!HasError(result.GetError()), FormatError(result.GetError()));
    return result.ExtractResult();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCompressionCodecTest)
{
    Y_UNIT_TEST(ShouldKeepStoredValues)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            static_cast<int>(NProto::COMPRESSION_CODEC_NONE));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            static_cast<int>(NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            static_cast<int>(NProto::COMPRESSION_CODEC_SNAPPY));
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            static_cast<int>(NProto::COMPRESSION_CODEC_ZSTD_3));
        UNIT_ASSERT_VALUES_EQUAL(
            4,
            static_cast<int>(NProto::COMPRESSION_CODEC_ZSTD_1));
    }

    Y_UNIT_TEST(ShouldParseCodecNames)
    {
        struct TCase
        {
            TStringBuf Name;
            NProto::ECompressionCodec Codec;
            TStringBuf CodecName;
        };

        const TCase cases[] = {
            {"lz4",
             NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
             "lz4-fast14-safe"},
            {"lz4-fast14-safe",
             NProto::COMPRESSION_CODEC_LZ4_FAST14_SAFE,
             "lz4-fast14-safe"},
            {"snappy", NProto::COMPRESSION_CODEC_SNAPPY, "snappy"},
            {"zstd", NProto::COMPRESSION_CODEC_ZSTD_3, "zstd_3"},
            {"zstd_3", NProto::COMPRESSION_CODEC_ZSTD_3, "zstd_3"},
            {"zstd_1", NProto::COMPRESSION_CODEC_ZSTD_1, "zstd_1"}};

        for (const auto& test: cases) {
            const auto codec = GetResultOrFail(ParseCompressionCodec(test.Name));
            UNIT_ASSERT_C(test.Codec == codec, test.Name);
            UNIT_ASSERT_C(
                NBlockCodecs::Codec(test.CodecName) ==
                    GetResultOrFail(GetCompressionCodec(codec)),
                test.Name);
        }
    }

    Y_UNIT_TEST(ShouldRejectUnsupportedCodecNames)
    {
        for (const TStringBuf name:
             {"",
              "unknown-codec",
              "null",
              "lz4-unknown",
              "lz4-fast14-fast",
              "lz4hc",
              "zstd_6",
              "zstd06_3",
              "zlib",
              "zlib-6",
              "lzma",
              "lzma-5",
              "bzip2",
              "bzip2-6",
              "fastlz",
              "fastlz-0",
              "brotli",
              "brotli_11"})
        {
            const auto error = ParseCompressionCodec(name).GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT(!error.GetMessage().empty());
        }
    }

    Y_UNIT_TEST(ShouldReturnCodecForEveryStoredValue)
    {
        const TString data(4096, 'a');

        for (int value = NProto::ECompressionCodec_MIN;
             value <= NProto::ECompressionCodec_MAX;
             ++value)
        {
            const auto codec = static_cast<NProto::ECompressionCodec>(value);
            if (codec == NProto::COMPRESSION_CODEC_NONE) {
                continue;
            }

            const auto* blockCodec = GetResultOrFail(GetCompressionCodec(codec));
            UNIT_ASSERT_C(blockCodec, value);
            UNIT_ASSERT_VALUES_EQUAL_C(
                data,
                blockCodec->Decode(blockCodec->Encode(data)),
                value);
        }
    }

    Y_UNIT_TEST(ShouldRejectUnknownStoredValues)
    {
        for (const int value: {0, -1, NProto::ECompressionCodec_MAX + 1, 1000}) {
            const auto error =
                GetCompressionCodec(static_cast<NProto::ECompressionCodec>(value))
                    .GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
        }
    }
}

}   // namespace NCloud
