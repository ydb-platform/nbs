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

#include <optional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

/**
 * Compresses blob payloads in independently decodable chunks and restores
 * selected blocks from the compressed data.
 *
 * Each chunk contains whole blocks; only the final chunk may be shorter than
 * the configured chunk size. Compressed chunks are concatenated in blob order,
 * with the N-1 internal boundaries, codec and uncompressed chunk size stored
 * in metadata. The final chunk ends at the compressed blob size.
 * PlanReadBlob selects the byte ranges needed for a read, and DecompressBlob
 * restores the requested blocks from those ranges.
 *
 * The constructor's chunk size and codec configure writes. Reads use the blob's
 * metadata and require the same block size that was used for compression.
 * Invalid inputs, compressed data and codec failures are returned as errors.
 */
class TBlobsCompressor
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

    struct TCompressedBlob
    {
        // Compressed chunks concatenated in their original order.
        TString Data;

        // Codec, uncompressed chunk size and N-1 cumulative end offsets.
        NProto::TCompressionMetadata Compression;
    };

    struct TReadPlan
    {
        // Query storage; only the first QueriesCount entries are used.
        TArrayHolder<NKikimr::TEvBlobStorage::TEvGet::TQuery> Queries;

        // Number of initialized queries, after deduplication and coalescing.
        size_t QueriesCount = 0;

        // Block offsets for each request.
        // ith element contains list of block offsets which depend on the ith
        // query.
        TVector<TVector<ui32>> BlockOffsetsForEachRequest;
    };

private:
    // Resolved once at construction; failures are returned by CompressBlob.
    const TResultOrError<const NBlockCodecs::ICodec*> BlobCodec;
    const std::optional<TMetricsConfig> MetricsConfig;

    // Size of one uncompressed block in bytes, used for both reads and writes.
    const ui32 BlockSize;

    // Uncompressed chunk size in bytes, used for writes.
    const std::optional<ui32> ChunkSize;

public:
    /**
     * Stores the block size and write settings and resolves the codec.
     * CompressBlob returns an error for invalid settings or codec lookup
     * errors.
     *
     * @param blockSize - Nonzero size of one uncompressed block in bytes.
     * @param chunkSize - Uncompressed chunk size in bytes; must be a multiple
     *  of and at least blockSize.
     * @param codecName - Codec name used for writes and compression metrics.
     * @param metricsConfig - Optional settings for compression estimation.
     *  Omitting them disables metrics.
     */
    TBlobsCompressor(
        ui32 blockSize,
        ui32 chunkSize,
        TStringBuf codecName,
        std::optional<TMetricsConfig> metricsConfig);

    /**
     * Constructs a blob compressor without write settings. It can plan reads,
     * decompress blobs and calculate compression metrics.
     *
     * @param blockSize - Nonzero size of one uncompressed block in bytes.
     * @param codecName - Codec name used for compression metrics. Reads use
     *  the codec recorded in the blob's metadata.
     * @param metricsConfig - Optional settings for compression estimation.
     *  Omitting them disables metrics.
     */
    TBlobsCompressor(
        ui32 blockSize,
        TStringBuf codecName,
        std::optional<TMetricsConfig> metricsConfig);

    /**
     * Compresses a blob using the configured chunk size and codec. The final
     * chunk may be shorter than ChunkSize. Returns an error if the settings
     * or payload are invalid, or compression fails.
     *
     * @param blobPayload - Uncompressed blocks in blob order, with one buffer
     *  of exactly BlockSize bytes per block. Null data represents a zero block.
     *
     * @return - Concatenated compressed chunks and their metadata, or an error.
     *  Invalid arguments return E_ARGUMENT; codec failures return E_FAIL.
     *  An empty payload produces empty data and no chunks, with codec and chunk
     *  size still recorded in the metadata.
     */
    [[nodiscard]] TResultOrError<TCompressedBlob> CompressBlob(
        const TSgList& blobPayload) const;

    /**
     * Restores requested blocks using the blob's codec and chunk size. Each
     * requested chunk is decoded once, even if several blocks refer to it.
     * Returns an error for invalid inputs or decoding failures.
     * Output buffers may already have been written when a failure occurs.
     *
     * @param compressedBuffers - Data from the ranges returned by PlanReadBlob
     *  for these block offsets, concatenated in query order. Each rope must
     *  contain only complete compressed chunks, which may span rope segments.
     *  Empty ropes are allowed. Each compressed chunk is extracted into a
     *  reusable buffer before decoding.
     * @param outputBuffers - One buffer of exactly BlockSize bytes
     *  per requested block, in blockOffsets order.
     *  Buffers must be non-null, writable and must not overlap input.
     * @param compression - Metadata describing the compressed blob.
     * @param blobSize - Total compressed blob size, including unread chunks.
     * @param blockOffsets - Block positions within the original blob, sorted
     *  in strictly increasing order (no duplicates). Every offset must refer
     *  to an existing block.
     * @param blockChecksums - Optional output; appends one checksum per block
     *  in blockOffsets order.
     *
     * @return - Success, E_ARGUMENT for invalid settings, metadata, offsets or
     *  output buffers, or E_REJECTED for invalid compressed data or decoding
     *  failures. Codec lookup failures return E_FAIL.
     */
    [[nodiscard]] NProto::TError DecompressBlob(
        const TVector<TRope>& compressedBuffers,
        const TSgList& outputBuffers,
        const NProto::TCompressionMetadata& compression,
        ui32 blobSize,
        const TVector<ui16>& blockOffsets,
        TVector<ui32>* blockChecksums) const;

    /**
     * Plans reads of the complete compressed chunks needed for the requested
     * blocks, using the chunk size from the blob's metadata. Each requested
     * chunk is read once and adjacent chunks are coalesced into a single range.
     * Returns E_ARGUMENT for invalid chunk metadata, offsets outside the chunk
     * list or read ranges extending beyond the compressed blob.
     *
     * The caller must supply valid block offsets: the metadata does not store
     * the original blob size, so this method cannot detect offsets beyond the
     * end of a partial final chunk.
     *
     * @param blobId - ID of the compressed blob to read.
     * @param compression - Metadata describing the compressed blob.
     * @param blockOffsets - Block positions within the original blob, sorted
     *  in strictly increasing order (no duplicates).
     *
     * @return - A read plan or an error. The plan contains an owning query
     * array and the number of initialized entries. Queries are for blobId,
     * sorted by byte offset (Shift), and cover disjoint ranges of complete
     * compressed chunks. Storage is allocated for at most blockOffsets.size()
     * queries; only the first QueriesCount entries are used. An empty block
     * list returns a null array and zero count without validating the blob ID
     * or metadata.
     */
    [[nodiscard]] TResultOrError<TReadPlan> PlanReadBlob(
        const NKikimr::TLogoBlobID& blobId,
        const NProto::TCompressionMetadata& compression,
        const TVector<ui16>& blockOffsets) const;

    /**
     * Calculates compression metrics for the given blob.
     *
     * @param blobId - ID of the compressed blob.
     * @param blobContent - Content of the compressed blob.
     */
    void CalculateCompressionMetrics(
        const TPartialBlobId& blobId,
        TStringBuf blobContent) const;

private:
    ui64 EstimateCompressedSize(TStringBuf blobContent, ui64 chunkSize) const;
};

}   // namespace NCloud::NBlockStore::NStorage
