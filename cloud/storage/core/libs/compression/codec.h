#pragma once

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/compression.pb.h>

#include <library/cpp/blockcodecs/codecs.h>

#include <util/generic/strbuf.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// Maps a storage configuration codec family (or a codec name) to the codec
// stored in blob metadata. A family selects its default codec: lz4 ->
// lz4-fast14-safe, snappy -> snappy, zstd -> zstd_3. Unsupported names return
// E_ARGUMENT.
TResultOrError<NProto::ECompressionCodec> ParseCompressionCodec(
    TStringBuf codecName);

// Returns the library/cpp/blockcodecs codec for the codec stored in blob
// metadata. COMPRESSION_CODEC_NONE and unknown values return E_ARGUMENT.
TResultOrError<const NBlockCodecs::ICodec*> GetCompressionCodec(
    NProto::ECompressionCodec codec);

}   // namespace NCloud
