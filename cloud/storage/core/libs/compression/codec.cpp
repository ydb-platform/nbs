#include "codec.h"

#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NCloud {

using namespace NProto;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCompressionCodec
{
    ECompressionCodec Codec;
    // Empty if the codec is not the default one of its family.
    TStringBuf Family;
    TStringBuf Name;
};

constexpr TCompressionCodec CompressionCodecs[] = {
    {COMPRESSION_CODEC_LZ4_FAST14_SAFE, "lz4", "lz4-fast14-safe"},
    {COMPRESSION_CODEC_SNAPPY, "snappy", "snappy"},
    {COMPRESSION_CODEC_ZSTD_3, "zstd", "zstd_3"},
    {COMPRESSION_CODEC_ZSTD_1, "", "zstd_1"}};

static_assert(
    ECompressionCodec_ARRAYSIZE - 1 ==
    sizeof(CompressionCodecs) / sizeof(CompressionCodecs[0]));

constexpr bool IsValidCodecList(const TCompressionCodec* codecList, size_t size)
{
    for (size_t i = 0; i < size; ++i) {
        if (codecList[i].Codec != static_cast<ECompressionCodec>(i + 1)) {
            return false;
        }
    }
    return true;
}

static_assert(
    IsValidCodecList(CompressionCodecs, ECompressionCodec_ARRAYSIZE - 1));

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<ECompressionCodec> ParseCompressionCodec(TStringBuf codecName)
{
    if (codecName) {
        for (const auto& entry: CompressionCodecs) {
            if (entry.Family == codecName || entry.Name == codecName) {
                return entry.Codec;
            }
        }
    }
    return MakeError(
        E_ARGUMENT,
        TStringBuilder() << "Unsupported blob compression codec: "
                         << codecName);
}

TResultOrError<const NBlockCodecs::ICodec*> GetCompressionCodec(
    ECompressionCodec codec)
{
    try {
        if (codec < ECompressionCodec_MIN || codec > ECompressionCodec_MAX ||
            codec == COMPRESSION_CODEC_NONE)
        {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Unsupported blob compression type: "
                                 << static_cast<int>(codec));
        }
        const auto& entry = CompressionCodecs[static_cast<size_t>(codec) - 1];

        return NBlockCodecs::Codec(entry.Name);
    } catch (const NBlockCodecs::TNotFound&) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Unavailable blob compression type: "
                             << static_cast<int>(codec));
    } catch (...) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "Failed to look up blob compression codec: "
                             << CurrentExceptionMessage());
    }
}

}   // namespace NCloud
