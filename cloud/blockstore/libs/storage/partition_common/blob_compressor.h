#pragma once

#include "part_counters_wrapper.h"

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/util/rope.h>

#include <library/cpp/blockcodecs/codecs.h>

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>
#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Maps a storage configuration codec family (or its default codec name) to the
// enum stored in blob metadata. Unsupported names return E_ARGUMENT.
TResultOrError<NProto::TCompressionMetadata::ECompressionType>
ParseBlobCompressionCodec(TStringBuf codecName);

struct TCompressedBlob
{
    // Compressed or raw chunks concatenated in their original order.
    TString Data;

    // Codec, chunk size and one stored size per chunk (zero for raw chunks).
    NProto::TCompressionMetadata Compression;
};

struct TReadPlan
{
    // Query storage with exactly QueriesCount entries.
    TArrayHolder<NKikimr::TEvBlobStorage::TEvGet::TQuery> Queries;

    // Number of initialized queries, after deduplication and coalescing.
    size_t QueriesCount = 0;

    // Block offsets for each request.
    // ith element contains list of block offsets which depend on the ith
    // query.
    TVector<TVector<ui16>> BlockOffsetsForEachRequest;
};

/**
 * Compresses a blob in independently decodable chunks using the codec resolved
 * from the given compression type.
 * Chunks contain whole blocks; only the final chunk may be shorter than
 * chunkSize. Chunks whose encoded size is at least their original size are
 * stored raw and marked by a zero size entry in the metadata.
 * Chunks are concatenated in blob order. Returns an error if the settings
 * or payload are invalid, or compression fails.
 *
 * @param blockSize - Nonzero size of one uncompressed block in bytes.
 * @param chunkSize - Uncompressed chunk size in bytes; must be a multiple
 *  of and at least blockSize.
 * @param compressionType - Compression type used to select the codec and
 *  recorded in the blob metadata.
 * @param blobPayload - Uncompressed blocks in blob order, with one buffer
 *  of exactly blockSize bytes per block. Null data represents a zero block.
 *
 * @return - Concatenated stored chunks and their metadata, or an error.
 *  Invalid arguments return E_ARGUMENT; codec failures return E_FAIL.
 *  An empty payload produces empty data and no chunks, with codec and chunk
 *  size still recorded in the metadata.
 */
[[nodiscard]] TResultOrError<TCompressedBlob> CompressBlob(
    ui32 blockSize,
    ui32 chunkSize,
    NProto::TCompressionMetadata::ECompressionType compressionType,
    const TSgList& blobPayload);

/**
 * Restores requested blocks using the blob's codec and chunk size. Each
 * requested compressed chunk is decoded once, even if several blocks refer
 * to it. Requested raw blocks are copied directly to the output buffers.
 * Returns an error for invalid inputs or decoding failures.
 * Output buffers may already have been written when a failure occurs.
 *
 * @param blockSize - Block size used for compression.
 * @param compressedBuffers - Data from the ranges returned by PlanReadBlob
 *  for these block offsets, concatenated in query order. Raw chunks contribute
 *  only the requested blocks. Compressed chunks may span ropes and rope
 *  segments.
 *  Empty ropes are allowed. Each compressed chunk is extracted into a
 *  reusable buffer before decoding.
 * @param compression - Metadata describing the compressed blob.
 * @param blobSize - Total stored blob size, including unread chunks.
 * @param blockOffsets - Block positions within the original blob, sorted
 *  in strictly increasing order (no duplicates). Every offset must refer
 *  to an existing block.
 * @param outputBuffers - One buffer of exactly blockSize bytes
 *  per requested block, in blockOffsets order.
 *  Buffers must be non-null, writable and must not overlap input.
 * @param blockChecksums - Optional output; appends one checksum per block
 *  in blockOffsets order.
 *
 * @return - Success, E_ARGUMENT for invalid settings, metadata, offsets or
 *  output buffers, or E_REJECTED for invalid compressed data or decoding
 *  failures. Codec lookup failures return E_FAIL.
 */
[[nodiscard]] NProto::TError DecompressBlob(
    ui32 blockSize,
    const TVector<TRope>& compressedBuffers,
    const NProto::TCompressionMetadata& compression,
    ui32 blobSize,
    const TVector<ui16>& blockOffsets,
    const TSgList& outputBuffers,
    TVector<ui32>* blockChecksums);

/**
 * Plans reads of complete compressed chunks and only requested blocks from
 * raw chunks, using the blob's metadata. Each compressed chunk is read once
 * and adjacent byte ranges are coalesced into a single query.
 * Cumulative chunk offsets are rebuilt from sizes, with zero marking raw
 * chunks. Returns E_ARGUMENT for invalid chunk metadata, offsets outside the
 * chunk list or read ranges extending beyond the compressed blob.
 *
 * The caller must supply valid block offsets: the metadata does not store
 * the original blob size, so this function cannot detect offsets beyond the
 * end of a compressed partial final chunk. Raw chunk bounds are validated.
 *
 * @param blockSize - Block size used for compression.
 * @param blobId - ID of the compressed blob to read.
 * @param compression - Metadata describing the compressed blob.
 * @param blockOffsets - Block positions within the original blob, sorted
 *  in strictly increasing order (no duplicates).
 *
 * @return - A read plan or an error. The plan contains an owning query
 * array and the number of initialized entries. Queries are for blobId,
 * sorted by byte offset (Shift), and cover disjoint ranges of stored data.
 * The array has exactly QueriesCount entries after coalescing. An empty
 * block list returns a null array and zero count without validating the
 * blob ID or metadata.
 */
[[nodiscard]] TResultOrError<TReadPlan> PlanReadBlob(
    ui32 blockSize,
    const NKikimr::TLogoBlobID& blobId,
    const NProto::TCompressionMetadata& compression,
    const TVector<ui16>& blockOffsets);

////////////////////////////////////////////////////////////////////////////////

// Estimates compression sizes for sampled blobs and updates partition counters.
class TBlobCompressor
{
public:
    struct TMetricsConfig
    {
        // Sample every Nth blob; zero disables estimation.
        ui32 CompressionRate = 0;

        // Uncompressed chunk sizes to estimate; zero means the whole blob.
        TVector<ui64> ChunkSizes;

        // Required when estimation is enabled.
        TThreadSafePartCountersPtr PartCounters;
    };

private:
    // Resolved once at construction; lookup failures disable estimation.
    const TResultOrError<const NBlockCodecs::ICodec*> BlobCodec;
    const std::optional<TMetricsConfig> MetricsConfig;

public:
    // Omitting metricsConfig or setting its sampling rate to zero disables
    // estimation. The codec is used only for estimating compressed sizes.
    TBlobCompressor(
        NProto::TCompressionMetadata::ECompressionType codec,
        std::optional<TMetricsConfig> metricsConfig);

    /**
     * Calculates compression metrics for the given blob.
     *
     * @param blobId - ID used to select sampled blobs.
     * @param blobContent - Uncompressed content of the blob.
     */
    void CalculateCompressionMetrics(
        const TPartialBlobId& blobId,
        TStringBuf blobContent) const;
};

using TBlobCompressorPtr = std::shared_ptr<TBlobCompressor>;

}   // namespace NCloud::NBlockStore::NStorage
